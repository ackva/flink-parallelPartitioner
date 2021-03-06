package org.myorg.quickstart.partitioners.matchFunctions;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.BaseBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.partitioners.GraphPartitionerImpl;
import org.myorg.quickstart.utils.*;

import java.text.DecimalFormat;
import java.util.*;

import static java.lang.Math.toIntExact;

/**
 *
 * This match function uses the "on Timer" approach. When processElement() is called, it sets an event to a later watermark and processes the edge(s) later.
 * This shall help to reduce the overall "waiting" time with unnecessary loop iterations.
 *
 */

public class MatchFunctionWinHash2 extends KeyedBroadcastProcessFunction<Integer, Edge<Integer, Long>, Tuple2<HashMap<Integer, Integer>,Long>, Tuple2<Edge<Integer, NullValue>,Integer>> {

    long currentWatermarkBro = 1;
    long currentWatermarkEle = 1;
    long lastCheck;

    List<WinHashStateBig> completeStateListFORDEBUG = new ArrayList<>();
    List<WinHashStateBig> notCompleteStateListFORDEBUG = new ArrayList<>();
    List<ProcessStateWatermark> allStates = new ArrayList<>();
    private int stateCounter;
    private int uncompleteStateCounter;
    long broadcastWatermark = 1;
    private int totalEdgesBroadcasted = 0;
    int totalWaitingEdgesCalls;
    List<Double> ratioRemainingInWaitingList = new ArrayList<>();
    List<Long> watermarksBro = new ArrayList<>();
    List<Long> watermarksEle = new ArrayList<>();
    private HashMap<Long,List<Edge>> watermarkState = new HashMap<>();
    private HashMap<Long,Boolean> watermarkReady = new HashMap<>();
    private int totalRepetitions = 0;
    private int collectedEdges = 0;
    private int stillNotInside = 0;
    private HashMap<String,Long> outputEdges = new HashMap<>();
    private List<Edge> duplicates = new ArrayList<>();
    private DecimalFormat df = new DecimalFormat("#.###");
    private int onTimerCount = 0;
    private int addedInState = 0;
    private int addedDirectly = 0;
    private long savedWatermark = 0;
    private int countBroadcastsOnWorker = 0;
    private int counterEdgesInstance = 0;
    int avgWaitingEdges = 0;
    int totalEdgesInWait = 0;
    long totalTimeBroadcast = 0;
    long totalTimeWaitingEdges = 0;
    long broadcastResetCounter = 1;
    private long globalCounterForPrint = 0;
    private String algorithm;
    private HashMap<Integer, Integer> vertexDegreeMap = new HashMap<>();
    private ModelBuilderGellyLong modelBuilder;
    private List<Edge<Integer, Long>> waitingEdges;
    private long startTime = System.currentTimeMillis();
    private int stateDelay = 0;
    /** The state that is maintained by this process function */
    private ValueState<ProcessStateWatermark> state;
    private int parallelism;
    List<Long> totalTimesStateStateCompletion = new ArrayList<>();
    private long highestWatermark;
    private HashMap<Long, WinHashStateBig> windowStateMap = new HashMap<>();
    private HashSet<WinHashStateBig> completeStateList = new HashSet<>();


    private ListState<ProcessState> state1;
   //MapStateDescriptor<String, Tuple2<Long,Boolean>> updatedStateDescriptor = new MapStateDescriptor<>("keineAhnung", BasicTypeInfo.STRING_TYPE_INFO, ValueTypeInfo.BOOLEAN_VALUE_TYPE_INFO);
    private int nullCounter = 0;


    public MatchFunctionWinHash2(String algorithm, Integer k, double lambda) {
        this.algorithm = algorithm;
        this.stateDelay = stateDelay;
        this.parallelism = k;
        this.waitingEdges = new ArrayList<>();
        if (algorithm.equals("hdrf"))
            this.modelBuilder = new ModelBuilderGellyLong(algorithm, vertexDegreeMap, k, lambda);
        if (algorithm.equals("dbh"))
            this.modelBuilder = new ModelBuilderGellyLong(algorithm, vertexDegreeMap, k);
    }

    // This function is called every time when a broadcast state is processed from the previous phase
    @Override
    public void processBroadcastElement(Tuple2<HashMap<Integer, Integer>,Long> broadcastElement, Context ctx, Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {

        countBroadcastsOnWorker++;

        long hashValue = broadcastElement.f1;

        int edgeDegreeInBroadcast = 0;

        // ### Merge local model from Phase 1 with global model, here in Phase 2
        int degree;
        Iterator it = broadcastElement.f0.entrySet().iterator();
        while (it.hasNext()) {
                Map.Entry<Integer, Integer> stateEntry = (Map.Entry)it.next();
                long vertex = stateEntry.getKey();
                if (this.algorithm.equals("hdrf")) {
                    if (modelBuilder.getHdrf().getCurrentState().checkIfRecordExits(vertex)) {
                        degree = toIntExact(stateEntry.getValue()) + modelBuilder.getHdrf().getCurrentState().getRecord(vertex).getDegree();
                        modelBuilder.getHdrf().getCurrentState().getRecord(vertex).setDegree(degree);
                    } else {
                        modelBuilder.getHdrf().getCurrentState().addRecordWithDegree(vertex, toIntExact(stateEntry.getValue()));
                    }
                }
                if (this.algorithm.equals("dbh")) {
                    if (modelBuilder.getDbh().getCurrentState().checkIfRecordExits(vertex)) {
                        degree = toIntExact(stateEntry.getValue()) + modelBuilder.getDbh().getCurrentState().getRecord(vertex).getDegree();
                        modelBuilder.getDbh().getCurrentState().getRecord(vertex).setDegree(degree);
                    } else {
                        modelBuilder.getDbh().getCurrentState().addRecordWithDegree(vertex, toIntExact(stateEntry.getValue()));
                    }
                }
                edgeDegreeInBroadcast += stateEntry.getValue();
        }

        double edgesInBroadcast = (double) edgeDegreeInBroadcast / 2;

        updateState(hashValue,(int) edgesInBroadcast);

        emitAllReadyEdges(out);

        if (ctx.currentWatermark() - highestWatermark > 65000) {
                cleanState(ctx.currentWatermark());
                highestWatermark = ctx.currentWatermark();
            }

    }


    @Override
    public void processElement(Edge<Integer, Long> currentEdge, ReadOnlyContext ctx, Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {

        counterEdgesInstance++;

        updateState(currentEdge.f2,currentEdge);

        emitAllReadyEdges(out);


      if (collectedEdges % 20_000_000 == 0 && collectedEdges > 0) {
            ctx.output(GraphPartitionerImpl.outputTag,checkTimer());
            lastCheck = ctx.currentWatermark();
      }


    }

    private void emitAllReadyEdges(Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {

        List<WinHashStateBig> statesToBeRemoved = new ArrayList<>();
        for (WinHashStateBig winState : completeStateList) {
            List<Edge> edgesToBeRemoved = new ArrayList<>();
            for (Edge e : winState.getEdgeList()) {
                int partitionId = modelBuilder.choosePartition(e);
                e.setValue(NullValue.getInstance());
                out.collect(new Tuple2<>(e,partitionId));
                collectedEdges++;
                edgesToBeRemoved.add(e);
             }
            if (winState.getEdgeList().size() != edgesToBeRemoved.size()) {
                System.out.println("not all edges removed (emitAllReadyEdges)");
            } else {
                winState.clearEdgeList();
            }
            statesToBeRemoved.add(winState);
            windowStateMap.remove(winState);

        }
        completeStateList.removeAll(statesToBeRemoved);

    }

    private void cleanState(long watermark) {
        int uselessBroadcastCounter = 0;
        List<WinHashStateBig> statesToBeRemoved = new ArrayList<>();

        for (WinHashStateBig w : windowStateMap.values()) {
            if (collectedEdges > 100_000 && w.getCreatedBy().equals("bro") && !w.isComplete() && !w.isUpdated() && (watermark - w.getCreateWatermark()) > 60000 && watermark > 0) {
                //System.out.println((watermark - w.getCreateWatermark()) + " diff for state " + w.getKey() + " created by " + w.getCreatedBy());
                uselessBroadcastCounter++;
                statesToBeRemoved.add(w);

            }
        }
        System.out.println("Useless broadcasts: " + uselessBroadcastCounter);

        for (WinHashStateBig winState : statesToBeRemoved) {
            windowStateMap.remove(winState.getKey());

        }


    }

    private int completeCounter = 0;

    private void updateState(long hashvalue, int size) throws Exception {
        WinHashStateBig winState;
        if (windowStateMap.containsKey(hashvalue)) {
            winState = windowStateMap.get(hashvalue);
            boolean complete = winState.addBroadcast(size);
            winState.setUpdated(true);
            //System.out.println(" new size for state " + winState.getKey() + " with size" + winState.getSize() + " having " + winState.getEdgeList().size() + " edges");
            if (complete) {
                addStateToReadyList(winState);
            }
        } else {
            winState = new WinHashStateBig(hashvalue,size);
            windowStateMap.put(hashvalue,winState);
            //stateCounter++;
            //System.out.println("# of states: " + stateCounter + "# of broadcasts: " + countBroadcastsOnWorker + "# of edges: " + counterEdgesInstance);
      }

    }

    private void updateState(long hashvalue, Edge edge) throws Exception {

        WinHashStateBig winState;
        if (windowStateMap.containsKey(hashvalue)) {
            winState = windowStateMap.get(hashvalue);
            //System.out.println(" new edge " + edge.f0 + "," + edge.f1 + " for state " + winState.getKey() + " with size" + winState.getSize());
            boolean complete = winState.addEdge(edge);
            winState.setUpdated(true);
            if (complete) {
                addStateToReadyList(winState);
            } else {

            }
        } else {
            winState = new WinHashStateBig(hashvalue,edge);
            windowStateMap.put(hashvalue,winState);
            if (TEMPGLOBALVARIABLES.printTime) {
                //notCompleteStateListFORDEBUG.add(winState);
                stateCounter++;
                //uncompleteStateCounter++;
            }

            //System.out.println(stateCounter + "# - adding state " + edge.f0 + "," + edge.f1 + " to state " + winState.getKey() + " with size" + winState.getSize());
        }

    }

    public void addStateToReadyList(WinHashStateBig winState) {
        completeCounter++;
        uncompleteStateCounter--;
        completeStateList.add(winState);

/*        // debugging
        if (TEMPGLOBALVARIABLES.printTime) {
            notCompleteStateListFORDEBUG.remove(winState);
            completeStateListFORDEBUG.add(winState);
        }*/

    }



    // debug
    public String checkProgressDEBUG() {
        String returnString = "";
        double sumUpdateTime = 0.0;
        double sumCompleteTime = 0.0;
        double completeCounter = 0.0;
        double updateCounter = 0.0;

            for (WinHashStateBig w : completeStateListFORDEBUG) {
                returnString = returnString + "CO: " + w.getKey() + " " + w.getTotalTime() + " " + w.getSize() + " ;; ";
                //totalTimesStateStateCompletion.add(w.getTotalTime());
                sumCompleteTime += w.getTotalTime();
                completeCounter++;
            }
        returnString = returnString + "AVG Complete Time = " + sumCompleteTime/completeCounter + " (complete: " + completeCounter + ") ;;";

            for (WinHashStateBig w : notCompleteStateListFORDEBUG) {
                returnString = returnString + w.getKey() + " " + w.getTotalTime() + " " + w.getSize() + " ;; ";
                //totalTimesStateStateCompletion.add(w.getUpdatetime());
                sumUpdateTime += w.getUpdatetime();
                updateCounter++;
            }
            returnString = returnString + "AVG UpdateTime = " + sumUpdateTime/updateCounter + " (incomplete: " + updateCounter + ") ;;";

            return returnString;
        }

    public static void Percentile(List<Long> latencies, double Percentile) {
        Collections.sort(latencies);
        System.out.print("p " + Percentile + ": " + (int)Math.ceil(((double)Percentile / (double)100) * (double)latencies.size()) + " ---- ");
    }

    public String checkTimer() {
        long timeNow = System.currentTimeMillis();
        long difference = timeNow - startTime;
        return "MAT ; " + System.currentTimeMillis() + " ; "  + collectedEdges + " ; "  + difference/1000 + " ; " + counterEdgesInstance;
    }

}



