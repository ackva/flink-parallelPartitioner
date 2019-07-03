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

    List<WinHashState> completeStateListFORDEBUG = new ArrayList<>();
    List<WinHashState> notCompleteStateListFORDEBUG = new ArrayList<>();
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

    }

    @Override
    public void processElement(Edge<Integer, Long> currentEdge, ReadOnlyContext ctx, Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {

        counterEdgesInstance++;

        ProcessStateWatermark current = state.value();
        if (current == null) {
            current = new ProcessStateWatermark();
            allStates.add(current);
            current.key = ctx.currentWatermark();
            //System.out.println("new state: all states: " + allStates.size() + " - total edge counter " + counterEdgesInstance);
        } else {
            //System.out.println("state exists");
        }
        current.lastModified = ctx.currentWatermark();
        state.update(current);
        ctx.timerService().registerEventTimeTimer(current.lastModified + 10000);

        updateState(currentEdge.f2,currentEdge);

        emitAllReadyEdges(out);


/*        if (ctx.currentWatermark() > lastCheck) {
            System.out.println("new check" + ctx.currentWatermark());
            lastCheck = ctx.currentWatermark();

        }*/




    }

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", ProcessStateWatermark.class));


    }

/*    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Edge<Integer, NullValue>, Integer>> out) throws Exception {

        ProcessStateWatermark edgeState = state.value();
        onTimerCount++;

        emitAllReadyEdges(out);

    }*/


    private void emitAllReadyEdges(Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {
        List<WinHashState> statesToBeRemoved = new ArrayList<>();
        for (WinHashState winState : completeStateList) {
            List<Edge> edgesToBeRemoved = new ArrayList<>();
            for (Edge e : winState.getEdgeList()) {
                int partitionId = modelBuilder.choosePartition(e);
                int src = Integer.parseInt(e.f0.toString());
                int trg = Integer.parseInt(e.f1.toString());
                out.collect(new Tuple2<>(new Edge<>(src,trg, NullValue.getInstance()),partitionId));
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



/*    private void checkWatermark(long watermark) {
        if (broadcastWatermark != watermark) {
            watermarksBro.add(watermark);
            broadcastWatermark = watermark;
            //System.out.println("DEG _ new Watermark = " + broadcastWatermark + " old: " + watermarks);
        }
        if (watermark == Long.MAX_VALUE) {
            System.out.println("Watermarks: " + watermarksBro.size() + " -- " + watermarksBro);
        }
    }*/

    private HashMap<Long, WinHashState> windowStateMap = new HashMap<>();
    private HashSet<WinHashState> completeStateList = new HashSet<>();

    private int completeCounter = 0;

    //private HashMap<Long, Integer> arrivedElemDiffSysTime = new HashMap<>();


    private void updateState(long hashvalue, int size) throws Exception {
        WinHashState winState;
        if (windowStateMap.containsKey(hashvalue)) {
            winState = windowStateMap.get(hashvalue);
            boolean complete = winState.addBroadcast(size);
            winState.setUpdated(true);
            //System.out.println(" new size for state " + winState.getKey() + " with size" + winState.getSize() + " having " + winState.getEdgeList().size() + " edges");
            if (complete) {
                addStateToReadyList(winState);
            }
        } else {
            winState = new WinHashState(hashvalue,size);
            windowStateMap.put(hashvalue,winState);
            //stateCounter++;
            //System.out.println("# of states: " + stateCounter + "# of broadcasts: " + countBroadcastsOnWorker + "# of edges: " + counterEdgesInstance);
      }

    }

    private void updateState(long hashvalue, Edge edge) throws Exception {

        WinHashState winState;
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
            winState = new WinHashState(hashvalue,edge);
            windowStateMap.put(hashvalue,winState);
            if (TEMPGLOBALVARIABLES.printTime) {
                //notCompleteStateListFORDEBUG.add(winState);
                stateCounter++;
                //uncompleteStateCounter++;
            }

            //System.out.println(stateCounter + "# - adding state " + edge.f0 + "," + edge.f1 + " to state " + winState.getKey() + " with size" + winState.getSize());
        }

    }

    public void addStateToReadyList(WinHashState winState) {
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

            for (WinHashState w : completeStateListFORDEBUG) {
                returnString = returnString + "CO: " + w.getKey() + " " + w.getTotalTime() + " " + w.getSize() + " ;; ";
                //totalTimesStateStateCompletion.add(w.getTotalTime());
                sumCompleteTime += w.getTotalTime();
                completeCounter++;
            }
        returnString = returnString + "AVG Complete Time = " + sumCompleteTime/completeCounter + " (complete: " + completeCounter + ") ;;";

            for (WinHashState w : notCompleteStateListFORDEBUG) {
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

    private boolean checkIfEarlyArrived(Edge<Integer, Long> currentEdge) {

        //System.out.println(vertexDegreeMap.containsKey(currentEdge.getEdge().f0) + " " + currentEdge.getEdge().f0 + " __ " + currentEdge.getEdge().f1 + vertexDegreeMap.containsKey(currentEdge.getEdge().f1));
        boolean sourceInside = false;
        boolean targetInside = false;
        if (algorithm.equals("hdrf")) {
            sourceInside = modelBuilder.getHdrf().getCurrentState().checkIfRecordExits(new Long(currentEdge.f0));
            targetInside = modelBuilder.getHdrf().getCurrentState().checkIfRecordExits(new Long(currentEdge.f1));
        } else if (algorithm.equals("dbh")) {
            sourceInside = modelBuilder.getDbh().getCurrentState().checkIfRecordExits(new Long(currentEdge.f0));
            targetInside = modelBuilder.getDbh().getCurrentState().checkIfRecordExits(new Long(currentEdge.f1));
        }

        // Debugging only
        if (TEMPGLOBALVARIABLES.printPhaseTwo) {
            //List<Tuple3> printState = modelBuilder.getHdrf().getCurrentState().printState();
            //System.out.println("Edge (" + currentEdge.getEdge().f0 + " " + currentEdge.getEdge().f1 + "): " + printState);
        }

        // Return TRUE if both vertices are in HashMap. Otherwise return false
        if (sourceInside && targetInside) {
            return true;
        } else {
            return false;
        }

    }

    public String checkTimer() {
        long timeNow = System.currentTimeMillis();
        long difference = timeNow - startTime;
        return "MAT > " + System.currentTimeMillis() + " > "  + collectedEdges + " > "  + difference/1000 + " > " + counterEdgesInstance;
    }

}



