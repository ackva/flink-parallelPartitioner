package org.myorg.quickstart.partitioners.matchFunctions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.co.BaseBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.IntValue;
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

public class MatchFunctionWinHash extends KeyedBroadcastProcessFunction<Integer, Edge<Integer, Long>, Tuple2<HashMap<Integer, Integer>,Long>, Tuple2<Edge<Integer, NullValue>,Integer>> {

    long currentWatermarkBro = 1;
    long currentWatermarkEle = 1;
    private HashMap<Long,Integer> repeatMap = new HashMap<>();
    private HashMap<ProcessStateLong,Long> allStates = new HashMap<>();
    private List<ProcessStateLong> allStateList = new ArrayList<>();

    boolean lastcall;
    int edgeOutputCount;
    List<WinHashState> completeStateListFORDEBUG = new ArrayList<>();
    List<WinHashState> notCompleteStateListFORDEBUG = new ArrayList<>();
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
    private ValueState<ProcessStateLong> state;
    private int parallelism;
    List<Long> totalTimesStateStateCompletion = new ArrayList<>();


    // private ListState<ProcessState> state1;
   //MapStateDescriptor<String, Tuple2<Long,Boolean>> updatedStateDescriptor = new MapStateDescriptor<>("keineAhnung", BasicTypeInfo.STRING_TYPE_INFO, ValueTypeInfo.BOOLEAN_VALUE_TYPE_INFO);
    private int nullCounter = 0;


    public MatchFunctionWinHash(String algorithm, Integer k, double lambda) {
        this.algorithm = algorithm;
        this.stateDelay = stateDelay;
        this.parallelism = k;
        this.waitingEdges = new ArrayList<>();
        if (algorithm.equals("hdrf"))
            this.modelBuilder = new ModelBuilderGellyLong(algorithm, vertexDegreeMap, k, lambda);
        if (algorithm.equals("dbh"))
            this.modelBuilder = new ModelBuilderGellyLong(algorithm, vertexDegreeMap, k);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", ProcessStateLong.class));
        //System.out.println("state delay: " + stateDelay);
    }


    // This function is called every time when a broadcast state is processed from the previous phase
    @Override
    public void processBroadcastElement(Tuple2<HashMap<Integer, Integer>,Long> broadcastElement, Context ctx, Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {

        long hashValue = broadcastElement.f1;

        globalCounterForPrint++;




        /*if (TEMPGLOBALVARIABLES.printTime) {
            if (globalCounterForPrint > 2_000)
            ctx.output(GraphPartitionerImpl.outputTag,"broadcasts " + globalCounterForPrint);
        }*/

        int edgeDegreeInBroadcast = 0;

        //double edgesInBroadcast = 0;

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
        //System.out.println(broadcastElement.f0 + " -- size " + " -- " + edgesInBroadcast);

        updateState(hashValue,(int) edgesInBroadcast);

        // debugging progress
            //ctx.output(GraphPartitionerImpl.outputTag,checkProgressDEBUG());

        emitAllReadyEdges(out);

/*        if (uncompleteStateCounter > 0 && collectedEdges > 345000) {
            ctx.output(GraphPartitionerImpl.outputTag,"EDGE progress: " + checkUncompleteDEBUG());
            String progress = checkTimer();
            ctx.output(GraphPartitionerImpl.outputTag, progress); // " --- " + df.format(((double) totalEdgesBroadcasted/parallelism) / (double) counterEdgesInstance) + " diff broadcasted/processed");
        }*/



        //ctx.output(GraphPartitionerImpl.outputTag,"BROAD > " + broadcastElement.f1 + " > " + edgesInBroadcast +  " > " + broadcastElement.f0);
        //checkDifference(broadcastElement.f1, 0,ctx.currentWatermark(),ctx.currentProcessingTime(), edgeDegreeInBroadcast, broadcastElement.f0.toString());

/*        long waitEdgesTime = 0;
        if (waitingEdges.size() > 0) {
            totalWaitingEdgesCalls++;
            long startTime2 = System.nanoTime();
            List<Edge<Integer, Long>> toBeRemoved = new ArrayList<>();
            for (Edge<Integer, Long> e : waitingEdges) {
                if (checkIfEarlyArrived(e)) {
                    int partitionId = modelBuilder.choosePartition(e);
                    out.collect(new Tuple2<>(e, partitionId));
                    edgeOutputCount++;
                    toBeRemoved.add(e);
                } else {
                    break;
                }
            }
            double ratioRemainingInWaitingEdges = (waitingEdges.size() - toBeRemoved.size()) / waitingEdges.size();
            //ctx.output(GraphPartitionerImpl.outputTag,(waitingEdges.size()-toBeRemoved.size()) + " remain from " + waitingEdges.size());
            waitingEdges.removeAll(toBeRemoved);

            if (TEMPGLOBALVARIABLES.printTime) {
                //double ratioRemainingInWaitingEdges = (waitingEdges.size() - toBeRemoved.size()) / waitingEdges.size();
                ratioRemainingInWaitingList.add(ratioRemainingInWaitingEdges);
                if (ratioRemainingInWaitingEdges < 1 && ratioRemainingInWaitingEdges > 0)
                    System.out.print(ratioRemainingInWaitingEdges * 100 + "%; ");

                //ctx.output(GraphPartitionerImpl.outputTag, "REL > " + toBeRemoved.size() + " > " + globalCounterForPrint);
                long endTime2 = System.nanoTime();
                waitEdgesTime = endTime2 - startTime2;
            }
        }*/


    }

    @Override
    public void processElement(Edge<Integer, Long> currentEdge, ReadOnlyContext ctx, Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {

        counterEdgesInstance++;

        updateState(currentEdge.f2,currentEdge);

        // debugging progress
        //ctx.output(GraphPartitionerImpl.outputTag,checkProgressDEBUG());

        emitAllReadyEdges(out);

        /*if (counterEdgesInstance % 1_000_000 == 0)
            ctx.output(GraphPartitionerImpl.outputTag, checkTimer());*/

/*
        if (uncompleteStateCounter > 0 && collectedEdges > 345000) {
            ctx.output(GraphPartitionerImpl.outputTag,"EDGE progress: " + checkUncompleteDEBUG());
            String progress = checkTimer();
            ctx.output(GraphPartitionerImpl.outputTag, progress); // " --- " + df.format(((double) totalEdgesBroadcasted/parallelism) / (double) counterEdgesInstance) + " diff broadcasted/processed");
        }*/


        if (TEMPGLOBALVARIABLES.printTime) {
            if (counterEdgesInstance < 2) {
                ctx.output(GraphPartitionerImpl.outputTag, "new Job started");
                //ctx.output(GraphPartitionerImpl.outputTag, "REP > CurrentWatermark > ratioRepeats > totalNumRepetitions > keysInRepeatMap > keyDistributionRepeat");
            }
            if (counterEdgesInstance % TEMPGLOBALVARIABLES.printModulo == 0) {
                String progress = checkTimer();
                ctx.output(GraphPartitionerImpl.outputTag, progress); // " --- " + df.format(((double) totalEdgesBroadcasted/parallelism) / (double) counterEdgesInstance) + " diff broadcasted/processed");
            }
            /*if (counterEdgesInstance % 670000 == 0) {
                String toPrint = "";
                Iterator it = windowStateMap.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<Integer, HashSet<Integer>> stateEntry = (Map.Entry)it.next();

                    if(vertexPartition.keySet().contains(stateEntry.getKey())) {
                        HashSet<Integer> partitionSet = vertexPartition.get(stateEntry.getKey());
                        for(Integer i: stateEntry.getValue())
                            partitionSet.add(i);
                        vertexPartition.put(stateEntry.getKey(),partitionSet);
                    } else {
                        vertexPartition.put(stateEntry.getKey(),stateEntry.getValue());
                    }
                }
                ctx.output(GraphPartitionerImpl.outputTag,


                        String progress = checkTimer();
                ctx.output(GraphPartitionerImpl.outputTag, progress); // " --- " + df.format(((double) totalEdgesBroadcasted/parallelism) / (double) counterEdgesInstance) + " diff broadcasted/processed");
            }*/
        }

    }

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
            if (TEMPGLOBALVARIABLES.printTime) {
                //notCompleteStateListFORDEBUG.add(winState);
                //stateCounter++;
            }

            //System.out.println(stateCounter + "# added state " + winState.getKey() + " in broadcast with size " + winState.getSize());
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
                notCompleteStateListFORDEBUG.add(winState);
                stateCounter++;
                uncompleteStateCounter++;
            }

            //System.out.println(stateCounter + "# - adding state " + edge.f0 + "," + edge.f1 + " to state " + winState.getKey() + " with size" + winState.getSize());
        }

    }

    public void addStateToReadyList(WinHashState winState) {
        completeCounter++;
        uncompleteStateCounter--;
        completeStateList.add(winState);

        // debugging
        if (TEMPGLOBALVARIABLES.printTime) {
            notCompleteStateListFORDEBUG.remove(winState);
            completeStateListFORDEBUG.add(winState);
        }

    }


    // debug
    public String checkUncompleteDEBUG() {
        String returnString = "NC total: " + uncompleteStateCounter;
        int createdByEle = 0;
        List<WinHashState> createdByEleList = new ArrayList<>();
        int createdByBro = 0;
        List<WinHashState> createdByBroList = new ArrayList<>();
        double sumUpdateTime = 0.0;
        double sumCompleteTime = 0.0;
        double avgCompleteTime = 0.0;
        double completeCounter = 0.0;
        double updateCounter = 0.0;

        for (WinHashState w : notCompleteStateListFORDEBUG) {
            if (w.getCreatedBy().equals("ele") && !w.isAddedToWatchList()) {
                createdByEle++;
                createdByEleList.add(w);
            } else {
                System.out.println("whoops not created by Ele " + w.getKey() + "  " + w.getEdgeList());
            }
        }

        returnString = returnString + ", by Ele : " + createdByEleList.size() + " ||| uncomplete (by ele) : ";

        for (WinHashState w : createdByEleList) {
            returnString = returnString + " (k=" + w.getKey() + " s=" +  w.getSize() +" t= " +  (System.currentTimeMillis() - w.getStarttime()) + " e= " + w.getEdgeList().get(0) + " listSize=" + w.getEdgeList().size() + ") ;;";
        }


        return returnString;
    }

    // debug
    public String checkProgressDEBUG() {
        String returnString = "";
        double avgUpdateTime = 0.0;
        double sumUpdateTime = 0.0;
        double sumCompleteTime = 0.0;
        double avgCompleteTime = 0.0;
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
            /*System.out.println("Size of not-completed " + totalTimesStateStateCompletion.size());
            Percentile(totalTimesStateStateCompletion, 25);
            Percentile(totalTimesStateStateCompletion, 50);
            Percentile(totalTimesStateStateCompletion, 75);
            Percentile(totalTimesStateStateCompletion, 100);*/

            return returnString;
        }


    public static void Percentile(List<Long> latencies, double Percentile) {
        Collections.sort(latencies);
        System.out.print("p " + Percentile + ": " + (int)Math.ceil(((double)Percentile / (double)100) * (double)latencies.size()) + " ---- ");
    }

/*        if (completeCounter % 1 == 0) {
            //System.out.println("complete: " + completeCounter + " out of " + windowStateMap.size());
            for (WinHashState w : completeStateListFORDEBUG) {
                System.out.println("- " + w.getKey());
            }
        }*/





      /*  avgDiffWatermark = arrivedDiffWaterMark.get(hashvalue) / edgeCounter;
        avgDiffWatermark = arrivedDiffProcessing.get(hashvalue) / edgeCounter;

        arrivedDiffWaterMark.put(hashvalue,-1L);
        arrivedDiffProcessing.put(hashvalue,-1L);*/


/*        System.out.println("Arrived First: " + arrivedFirst.size() + " --> " + arrivedFirst);
        System.out.println("Differences Watermarks : " + arrivedDiffWaterMark.size() + " --> " + arrivedDiffWaterMark);
        System.out.println("Differences Processing : " + arrivedDiffProcessing.size() + " --> " + arrivedDiffProcessing);
        System.out.println("Differences SystemTime : " + arrivedDiffProcessing.size() + " --> " + arrivedDiffSysTime);
        System.out.println("Avg Diff Watermarks: " + avgDiffWatermark);
        System.out.println("Avg Diff Processing: " + avgDiffProcessing);
        System.out.println("Avg Diff SystemTime: " + avgDiffSysTime);*/






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

    private Tuple2<Edge<Integer, Long>,Integer> emitEdge(Edge<Integer, Long> edge,int place, long currTime, BaseBroadcastProcessFunction.ReadOnlyContext ctx) throws Exception {
        int partitionId = modelBuilder.choosePartition(edge);
        collectedEdges++;
        //if (collectedEdges % 100000 == 0)
           // ctx.output(GraphPartitionerImpl.outputTag,ctx.broadcastWatermark() + "$ emitted$1$total$"+collectedEdges + "$" + edge + "$" + place + "$");
        if (place != 1)
            //ctx.output(GraphPartitionerImpl.outputTag,"WAITING__$1$total$"+collectedEdges + "$" + edge + "$" + place + "$$");
        if (outputEdges.containsKey(edge.toString())) {
            //ctx.output(GraphPartitionerImpl.outputTag,ctx.broadcastWatermark() + "$ " + edge + " -- " + duplicates.size() + " duplicate (delayed by) " + (outputEdges.get(edge.toString())-currTime));
            duplicates.add(edge);

            }
        outputEdges.put(edge.toString(),currTime);
        //System.out.println(collectedEdges);
        return new Tuple2<>(edge, partitionId);
    }


    private HashMap getRepetitionDistribution(HashMap allRepeats) {
        HashMap<Integer, Integer> repeatDistribution = new HashMap<>();
        Iterator it = allRepeats.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            int value = Integer.parseInt(pair.getValue().toString());
            if (repeatDistribution.containsKey(value)) {
                int oldVal = repeatDistribution.get(value);
                int newVal = oldVal + 1;
                repeatDistribution.replace(value, oldVal, newVal);
            } else {
                repeatDistribution.put(value, 1);
            }
        }
        return repeatDistribution;
    }


}



