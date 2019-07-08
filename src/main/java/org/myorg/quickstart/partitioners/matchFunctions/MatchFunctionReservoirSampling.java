package org.myorg.quickstart.partitioners.matchFunctions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.partitioners.GraphPartitionerImpl;
import org.myorg.quickstart.utils.*;

import java.util.*;

import static java.lang.Math.toIntExact;

/**
 *
 * This match function uses the Reservoir-Sampling approach.
 *
 */

public class MatchFunctionReservoirSampling extends KeyedBroadcastProcessFunction<Integer, Edge<Integer, Long>, Tuple2<HashMap<Integer, Integer>,Long>, Tuple2<Edge<Integer, NullValue>,Integer>> {

    private int vertexArrivalAfterTableFull = 0;
    private double avgStateCompletionTime = 0.0;
    private double totalCompletionTime = 0.0;
    private double totalInsertTimeBefore = 0.0;
    private double avgInsertTimeBefore = 0.0;
    private double avgInsertTimeAfter = 0.0;
    private double totalInsertTimeAfter = 0.0;

    private int collectedEdges = 0;
    private int counterEdgesInstance = 0;
    private String algorithm;
    private ModelBuilderFixedSize modelBuilder;
    private List<Edge<Integer, Long>> waitingEdges;
    private long startTime = System.currentTimeMillis();
    private ValueState<ProcessState2> state;
    List<Long> stateList = new ArrayList<>();
    private int parallelism;
    private int countBroadcastsOnWorker;
    List<Long> totalTimesStateStateCompletion = new ArrayList<>();
    private int notCompleteStatesFORDEBUG = 0;
    private int completeStatesFORDEBUG = 0;
    List<WinHashState> notCompleteStateListFORDEBUG = new ArrayList<>();
    List<WinHashState> completeStateListFORDEBUG = new ArrayList<>();
    private int sampleSize;
    private int stateCounter;
    private int stateExistsCounter;
    private long highestWatermark;
    private boolean timerIsSet;
    private boolean timerEmitCall;
    private int onTimerCount;

    //private ListState<Long> state;
   //MapStateDescriptor<String, Tuple2<Long,Boolean>> updatedStateDescriptor = new MapStateDescriptor<>("keineAhnung", BasicTypeInfo.STRING_TYPE_INFO, ValueTypeInfo.BOOLEAN_VALUE_TYPE_INFO);
    private int nullCounter = 0;


    public MatchFunctionReservoirSampling(String algorithm, Integer k, double lambda, int sampleSize) {
        //HashMap<Integer, Integer> vertexDegreeMap = new HashMap<>();
        this.algorithm = algorithm;
        this.parallelism = k;
        this.waitingEdges = new ArrayList<>();
        if (algorithm.equals("hdrf")) {
            this.modelBuilder = new ModelBuilderFixedSize(algorithm, k, lambda, sampleSize);

        }
        if (algorithm.equals("dbh")) {
            this.modelBuilder = new ModelBuilderFixedSize(algorithm, k, sampleSize);
        }
        this.sampleSize = sampleSize;
    }

    // This function is called every time when a broadcast state is processed from the previous phase
    @Override
    public void processBroadcastElement(Tuple2<HashMap<Integer, Integer>,Long> broadcastElement, Context ctx, Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {

        if (timerEmitCall) {
            ctx.output(GraphPartitionerImpl.outputTag,"ProcessElement called after lastTimer");
        }

        //  ProcessState2 valState = checkValueState(ctx.currentWatermark());

        //ctx.output(GraphPartitionerImpl.outputTag,"BROAD: " + ctx.currentWatermark() + " - " + broadcastElement);

        countBroadcastsOnWorker++;

       /* if (modelBuilder.getHdrf().getCurrentState().getRecord_map().size() > 249999) {
            System.out.println(collectedEdges + " collected -- broadcast - complete state count:" +  completeStatesFORDEBUG + " -- not complete state count: " + notCompleteStatesFORDEBUG);
        }*/

        long hashValue = broadcastElement.f1;

        int edgeDegreeInBroadcast = 0;

        // ### Merge local model from Phase 1 with global model, here in Phase 2
        int degree;

        for (Map.Entry<Integer, Integer> integerIntegerEntry : broadcastElement.f0.entrySet()) {
            Map.Entry<Integer, Integer> stateEntry = (Map.Entry) integerIntegerEntry;
            int vertex = stateEntry.getKey();
            if (this.algorithm.equals("hdrf")) {
                //System.out.println("vertex " + vertex + " in map? " + modelBuilder.getHdrf().getCurrentState().getRecord_map());
                //System.out.println("Trying replace - sample is filled - probability to stay: " + probabilityToReplace + " to be replaced flip coin: " + flipCoin + " new field: " + x + " with degree (passed: ");
                /*if (modelBuilder.getHdrf().getCurrentState().getRecord_map().size() > 249999) {
                    int highdegreecounter = 0;
                    for (Map.Entry<Long,StoredObjectFixedSize> entry: modelBuilder.getHdrf().getCurrentState().getRecord_map().entrySet()) {
                        if (entry.getValue().isHighDegree())
                            highdegreecounter++;
                    }
                    //System.out.println("high degree count: " + highdegreecounter);
                }*/
                if (modelBuilder.getHdrf().getCurrentState().checkIfRecordExits(vertex)) {
                    //StoredStateFixedSize model = modelBuilder.getHdrf().getCurrentState();
                    //StoredObjectFixedSize record = modelBuilder.getHdrf().getCurrentState().getRecord(vertex);
                    degree = toIntExact(stateEntry.getValue()) + modelBuilder.getHdrf().getCurrentState().getRecord(vertex).getDegree();
                    modelBuilder.getHdrf().getCurrentState().getRecord(vertex).setDegree(degree);
                    modelBuilder.getHdrf().getCurrentState().increaseTotalDegree(degree);
                    //System.out.println("AVG degree -- " + modelBuilder.getHdrf().getCurrentState().getAverageDegree());
                    //modelBuilder.getHdrf().getCurrentState().getRecord(vertex).checkHighDegree(modelBuilder.getHdrf().getCurrentState().getAverageDegree());
                } else {
                    long now = System.nanoTime();
                    modelBuilder.getHdrf().getCurrentState().addRecordWithReservoirSampling(vertex, toIntExact(stateEntry.getValue()));
           /*         if (modelBuilder.getHdrf().getCurrentState().getRecord_map().size() > (sampleSize - 500000) && modelBuilder.getHdrf().getCurrentState().getRecord_map().size() < (sampleSize)) {
                        totalInsertTimeBefore += System.nanoTime() - now;
                        avgInsertTimeBefore = totalInsertTimeBefore / (double) countBroadcastsOnWorker;
                        if (countBroadcastsOnWorker % 2 == 0)
                        System.out.println("adding with reservor sampling before full table took  avg " + avgInsertTimeBefore/1000000 + " ms. counter " + countBroadcastsOnWorker + " --- emitted edges: " + collectedEdges);
                    } else if (modelBuilder.getHdrf().getCurrentState().getRecord_map().size() == (sampleSize)) {
                        vertexArrivalAfterTableFull++;
                        totalInsertTimeAfter += System.nanoTime() - now;
                        avgInsertTimeAfter = totalInsertTimeAfter / (double) vertexArrivalAfterTableFull;
                        if (countBroadcastsOnWorker % 2 == 0)
                        System.out.println("adding with reservor sampling after full table took  avg " + avgInsertTimeAfter/1000000 + " ms. counter " + countBroadcastsOnWorker + " --- emitted edges: " + collectedEdges);;
                    }*/
                }
            }
            if (this.algorithm.equals("dbh")) {
                if (modelBuilder.getDbh().getCurrentState().checkIfRecordExits(vertex)) {
                    degree = toIntExact(stateEntry.getValue()) + modelBuilder.getDbh().getCurrentState().getRecord(vertex).getDegree();
                    modelBuilder.getDbh().getCurrentState().getRecord(vertex).setDegree(degree);
                } else {
                    modelBuilder.getDbh().getCurrentState().addRecordWithReservoirSampling(vertex, toIntExact(stateEntry.getValue()));
                }
            }
            edgeDegreeInBroadcast += stateEntry.getValue();
        }

        double edgesInBroadcast = (double) edgeDegreeInBroadcast / 2;

        updateState(hashValue,(int) edgesInBroadcast, ctx.currentWatermark());

        emitAllReadyEdges(out);

        int currentStateSize;
        if (this.algorithm.equals("hdrf")) {
            currentStateSize = modelBuilder.getHdrf().getCurrentState().getRecord_map().size();
        } else {
            currentStateSize = modelBuilder.getDbh().getCurrentState().getRecord_map().size();
        }

        if (currentStateSize == sampleSize) {
            if (ctx.currentWatermark() - highestWatermark > 30000) {
                cleanState(ctx.currentWatermark());
                highestWatermark = ctx.currentWatermark();
            }

        }



    }

    private void cleanState(long watermark) {
        int uselessBroadcastCounter = 0;
        List<WinHashState> statesToBeRemoved = new ArrayList<>();

        for (WinHashState w : windowStateMap.values()) {
            if (w.getCreatedBy().equals("bro") && !w.isComplete() && !w.isUpdated() && (watermark - w.getCreateWatermark()) > 20000) {
                //System.out.println((watermark - w.getCreateWatermark()) + " diff for state " + w.getKey() + " created by " + w.getCreatedBy());
                uselessBroadcastCounter++;
                statesToBeRemoved.add(w);
            }
        }
        System.out.println("Useless broadcasts: " + uselessBroadcastCounter);

        for (WinHashState winState : statesToBeRemoved) {
            windowStateMap.remove(winState.getKey());
        }


    }

    @Override
    public void processElement(Edge<Integer, Long> currentEdge, ReadOnlyContext ctx, Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {

/*        if (timerEmitCall) {
            ctx.output(GraphPartitionerImpl.outputTag,"ProcessElement called after lastTimer");
        }*/

/*        if (!timerIsSet) {
            this.timerIsSet = true;
            //long newTime = watermark+60000;
            //System.out.println(newTime + " ---- new time: " + (watermark+60000));
            //ctx.timerService().registerEventTimeTimer(newTime);
            //ctx.timerService().registerEventTimeTimer(3703500800000L);
            ctx.timerService().registerEventTimeTimer(3703500800000L);
            System.out.println("#### timer set");
        }*/
       // ctx.output(GraphPartitionerImpl.outputTag,"ELEM: " + ctx.currentWatermark() + " - " + currentEdge);
        //ProcessState2 valState = checkValueState(ctx.currentWatermark());
        //long newTime = ctx.currentWatermark()+60000;
        //System.out.println(newTime + " ---- new time: " + ctx.currentWatermark()+60000);

        //ctx.timerService().registerEventTimeTimer((ctx.currentWatermark() + 60000));
        //ctx.timerService().registerEventTimeTimer(3703500800000L);


        counterEdgesInstance++;

       /* if (modelBuilder.getHdrf().getCurrentState().getRecord_map().size() > 249999) {
            System.out.println(collectedEdges + " collected -- element - complete state count:" +  completeStatesFORDEBUG + " -- not complete state count: " + notCompleteStatesFORDEBUG);
        }*/

        updateState(currentEdge.f2,currentEdge, ctx.currentWatermark());

        emitAllReadyEdges(out);

        if (counterEdgesInstance > 0 && counterEdgesInstance % 100_000 == 0)
            ctx.output(GraphPartitionerImpl.outputTag,checkTimer() );

    }


    private void emitAllReadyEdges(Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {

        List<WinHashState> statesToBeRemoved = new ArrayList<>();
        for (WinHashState winState : completeStateList) {
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

       /* if (collectedEdges % 100000 == 0 && collectedEdges > 0)
            System.out.println(checkProgressDEBUG());*/


        /*if (collectedEdges % 100 == 0 && collectedEdges > 0) {
            System.out.println(modelBuilder.getHdrf().getCurrentState().getRecord_map().size() + " entries in State Map");
            System.out.println("State Map: " + modelBuilder.getHdrf().getCurrentState().printState());
            //System.out.println("Degree Map: " + modelBuilder.getHdrf().getCurrentState().getDegrees());
        }*/

       /* if (modelBuilder.getHdrf().getCurrentState().getRecord_map().size() > 24995) {
            System.out.println("edges collected: " + collectedEdges + " -- " + System.currentTimeMillis());
        }*/

    }

    private HashMap<Long, WinHashState> windowStateMap = new HashMap<>();
    private HashSet<WinHashState> completeStateList = new HashSet<>();

    private int completeStateCounter = 0;

    //private HashMap<Long, Integer> arrivedElemDiffSysTime = new HashMap<>();


    private void updateState(long hashvalue, int size, long watermark) throws Exception {
        WinHashState winState;
        if (windowStateMap.containsKey(hashvalue)) {
            winState = windowStateMap.get(hashvalue);
            boolean complete = winState.addBroadcast(size);
            winState.setUpdated(true);
            //System.out.println(" new size for state " + winState.getKey() + " with size" + winState.getSize() + " having " + winState.getEdgeList().size() + " edges");
            if (complete) {
                winState.setCompleteWatermark(watermark);
                addStateToReadyList(winState);
            }
        } else {
            winState = new WinHashState(hashvalue,size, watermark);
            windowStateMap.put(hashvalue,winState);
            //notCompleteStateListFORDEBUG.add(winState);
        }
    }

    private void updateState(long hashvalue, Edge edge, long watermark) throws Exception {

        WinHashState winState;
        if (windowStateMap.containsKey(hashvalue)) {
            winState = windowStateMap.get(hashvalue);
            //System.out.println(" new edge " + edge.f0 + "," + edge.f1 + " for state " + winState.getKey() + " with size" + winState.getSize());
            boolean complete = winState.addEdge(edge);
            winState.setUpdated(true);
            if (complete) {
                winState.setCompleteWatermark(watermark);
                addStateToReadyList(winState);
            } else {
                //notCompleteStateListFORDEBUG.add(winState);
            }
        } else {
            winState = new WinHashState(hashvalue,edge,watermark);
            windowStateMap.put(hashvalue,winState);
            //notCompleteStatesFORDEBUG++;


            //System.out.println("adding state " + edge.f0 + "," + edge.f1 + " to state " + winState.getKey() + " with size" + winState.getSize());
        }

    }

    private double avgTotalTimeAfterSample = 0.0;
    private double totalTimeAfterSameple = 0.0;
    private double completeStateAfterSample = 0.0;

    public void addStateToReadyList(WinHashState winState) {
        completeStateCounter++;
        completeStateList.add(winState);
        //completeStateListFORDEBUG.add(winState);
        //notCompleteStateListFORDEBUG.remove(winState);
        //totalCompletionTime = totalCompletionTime + winState.getTotalTime();
        //System.out.println(totalCompletionTime + " total time " + completeStateCounter);

        // turn on for debugging
        /*avgStateCompletionTime = totalCompletionTime/completeStateCounter;// (avgStateCompletionTime * (collectedEdges - 1) + winState.getTotalTime())/collectedEdges;
        if (collectedEdges % 20000 == 0)
         System.out.println("AVG state completion time" + avgStateCompletionTime);
        if (modelBuilder.getHdrf().getCurrentState().getRecord_map().size() > 249999) {
            System.out.println("AVG state completion time > 2500000" + avgStateCompletionTime);
            completeStateAfterSample++;
            totalTimeAfterSameple = totalTimeAfterSameple + winState.getTotalTime();
            avgTotalTimeAfterSample = totalTimeAfterSameple/completeStateAfterSample;
            System.out.println("new AVG state completion time " + avgStateCompletionTime);
        }*/
/*        completeStatesFORDEBUG++;
        notCompleteStatesFORDEBUG--;*/



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
        double neverUpdatedCounter = 0.0;
        double edgesWaitingForBroad = 0.0;


        for (WinHashState w : completeStateListFORDEBUG) {
                //returnString = returnString + "CO: " + w.getKey() + " " + w.getTotalTime() + " " + w.getSize() + " ;; ";
                //totalTimesStateStateCompletion.add(w.getTotalTime());
                sumCompleteTime += (double) w.getTotalTime();
                completeCounter++;
            }
        System.out.println(completeStateListFORDEBUG.size() + " completed states");
        returnString = returnString + "AVG Complete Time = " + sumCompleteTime/completeCounter + " (complete: " + completeCounter + ") ;;";

        String waiting = "";
            for (WinHashState w : notCompleteStateListFORDEBUG) {
                //returnString = returnString + w.getKey() + " " + w.isUpdated() + " " + w.getUpdatetime() + " " + w.getSize() + " ;; ";
                //totalTimesStateStateCompletion.add(w.getUpdatetime());
                if (w.getUpdatetime() == 0.0) {
                    neverUpdatedCounter++;
                } else if (w.getUpdatetime() > 0.0) {
                    sumUpdateTime += (double) w.getUpdatetime();
                    updateCounter++;
                }

                if (w.getSize() == -1) {
                    edgesWaitingForBroad++;
                    waiting = waiting + " [ " + w.getKey() + w.getStarttime() + w.getEdgeList() + " ] ;; ";
                }

            }
            returnString = returnString + "AVG UpdateTime = " + sumUpdateTime/updateCounter + " (updated: " + updateCounter + ") never updated:" + neverUpdatedCounter + " edgeCounter " + counterEdgesInstance + " size(-1) = "  + edgesWaitingForBroad + " .. waiting: " + waiting;


            return returnString;
        }

    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Edge<Integer, NullValue>, Integer>> out) throws Exception {

        long watermark = ctx.currentWatermark();
        onTimerCount++;
        //System.out.println("### Timer called " + watermark + " ||  edges collected now: " + collectedEdges);

        //if (watermark > highestWatermark && watermark < 3803500800000L) {
        /*if (!timerIsSet) {
            this.timerIsSet = true;
            long newTime = watermark+60000;
            System.out.println(newTime + " ---- new time: " + (watermark+60000));
            //ctx.timerService().registerEventTimeTimer(newTime);
            ctx.timerService().registerEventTimeTimer(3703500800000L);
            System.out.println("#### highest WM setnew highest + set new timer in onTimer");
            highestWatermark = ctx.currentWatermark();
        }*/
        /*if (watermark > 3803500800000L) {
            //System.out.println("#### cleanup now! edges collected now: " + collectedEdges);
            if (!timerEmitCall) {
                ctx.output(GraphPartitionerImpl.outputTag,"emitting edges in timer " + collectedEdges);
                emitAllReadyEdges(out);
                ctx.output(GraphPartitionerImpl.outputTag,checkProgressDEBUG());
                timerEmitCall = true;
            }

        }*/

    }

        /*

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

    }*/

    public String checkTimer() {
        long timeNow = System.currentTimeMillis();
        long difference = timeNow - startTime;
        return "MAT;" + difference/1000  + ";" + counterEdgesInstance + ";"  + collectedEdges + ";" + System.currentTimeMillis();
    }

}



