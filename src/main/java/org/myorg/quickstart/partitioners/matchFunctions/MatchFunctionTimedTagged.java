package org.myorg.quickstart.partitioners.matchFunctions;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.co.BaseBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
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

public class MatchFunctionTimedTagged extends KeyedBroadcastProcessFunction<Integer, Edge<Integer, Long>, Tuple2<HashMap<Integer, Integer>,Long>, Tuple2<Edge<Integer, Long>,Integer>> {

    private HashMap<Long,Integer> repeatMap = new HashMap<>();
    private HashMap<ProcessStateLong,Long> allStates = new HashMap<>();
    private List<ProcessStateLong> allStateList = new ArrayList<>();

    MapStateDescriptor<Long, Boolean> stateDescriptor = new MapStateDescriptor<>
            ("TestBroadCastState", BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(new TypeHint<Boolean>() {}));

    private final MapStateDescriptor<Long, Boolean> readyToGoStateDescriptor =
            new MapStateDescriptor<>(
                    "RulesBroadcastState",
                    BasicTypeInfo.LONG_TYPE_INFO,
                    TypeInformation.of(new TypeHint<Boolean>() {}));


    private HashMap<Long,Boolean> readyToGo = new HashMap<>();
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
   // private ListState<ProcessState> state1;
   //MapStateDescriptor<String, Tuple2<Long,Boolean>> updatedStateDescriptor = new MapStateDescriptor<>("keineAhnung", BasicTypeInfo.STRING_TYPE_INFO, ValueTypeInfo.BOOLEAN_VALUE_TYPE_INFO);


    public MatchFunctionTimedTagged(String algorithm, Integer k, double lambda, int stateDelay) {
        this.algorithm = algorithm;
        this.stateDelay = stateDelay;
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
    public void processBroadcastElement(Tuple2<HashMap<Integer, Integer>,Long> broadcastElement, Context ctx, Collector<Tuple2<Edge<Integer, Long>,Integer>> out) throws Exception {

        // ctx.output(GraphPartitionerImpl.outputTag,ctx.broadcastWatermark() + " > " + broadcastElement + " > BROADCAST ");


        globalCounterForPrint++;
        countBroadcastsOnWorker++;

        if (this.algorithm.equals("hdrf")) {
            // ### Merge local model from Phase 1 with global model, here in Phase 2
            Iterator it = broadcastElement.f0.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, Integer> stateEntry = (Map.Entry)it.next();
                long vertex = stateEntry.getKey();
                if(modelBuilder.getHdrf().getCurrentState().checkIfRecordExits(vertex)) {
                    //System.out.println("entry " + stateEntry + " exists: degree before: " + modelBuilder.getHdrf().getCurrentState().getRecord(stateEntry.getKey()).getDegree());
                    int degree = toIntExact(stateEntry.getValue()) + modelBuilder.getHdrf().getCurrentState().getRecord(vertex).getDegree();
                    modelBuilder.getHdrf().getCurrentState().getRecord(vertex).setDegree(degree);
                } else {
                    modelBuilder.getHdrf().getCurrentState().addRecordWithDegree(vertex, toIntExact(stateEntry.getValue()));
                }
            }
        }

        if (this.algorithm.equals("dbh")) {
            // ### Merge local model from Phase 1 with global model, here in Phase 2
            Iterator it = broadcastElement.f0.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, Integer> stateEntry = (Map.Entry)it.next();
                long vertex = stateEntry.getKey();
                if(modelBuilder.getDbh().getCurrentState().checkIfRecordExits(vertex)) {
                    //System.out.println("entry " + stateEntry + " exists: degree before: " + modelBuilder.getHdrf().getCurrentState().getRecord(stateEntry.getKey()).getDegree());
                    int degree = toIntExact(stateEntry.getValue()) + modelBuilder.getDbh().getCurrentState().getRecord(vertex).getDegree();
                    modelBuilder.getDbh().getCurrentState().getRecord(vertex).setDegree(degree);
                } else {
                    modelBuilder.getDbh().getCurrentState().addRecordWithDegree(vertex, toIntExact(stateEntry.getValue()));
                }
            }
        }

        readyToGo.put(broadcastElement.f1,true);
        ctx.getBroadcastState(readyToGoStateDescriptor).put(broadcastElement.f1,true);
        System.out.println("added to state: " + broadcastElement.f0);

/*        ProcessState testState = state.value();
        if (testState != null)
            ctx.output(GraphPartitionerImpl.outputTag,ctx.broadcastWatermark() + "$" + testState.key + " -- hello from broadie");*/



    }

    @Override
    public void processElement(Edge<Integer, Long> currentEdge, ReadOnlyContext ctx, Collector<Tuple2<Edge<Integer, Long>,Integer>> out) throws Exception {

       // ctx.output(GraphPartitionerImpl.outputTag,ctx.broadcastWatermark() + " > " + currentEdge + " > EDGE ARRIVAL");

        //ctx.getBroadcastState(readyToGoStateDescriptor).get(1L);
        //ctx.getBroadcastState(readyToGoStateDescriptor).

        counterEdgesInstance++;


        if (ctx.getBroadcastState(readyToGoStateDescriptor).contains(currentEdge.f2)) {
            ctx.output(GraphPartitionerImpl.outputTag,currentEdge + " let's go ");
            out.collect(emitEdge(currentEdge,0, System.currentTimeMillis(),ctx));
        } else {
            ctx.output(GraphPartitionerImpl.outputTag,currentEdge + " naah, let's wait then");
            ProcessStateLong current = state.value();
            if (current == null) {
                current = new ProcessStateLong();
                current.key = currentEdge.f2;
                state.update(current);
                allStateList.add(current);
                ctx.output(GraphPartitionerImpl.outputTag,"New state " + current.key + " -- " + current + " - Number of states: " + allStateList.size() + " --> ");
            } else {
            current.edgeList.add(currentEdge);
            current.lastModified = ctx.currentWatermark();
            state.update(current);
            // schedule the next timer X seconds from the current event time
            ctx.timerService().registerEventTimeTimer(current.lastModified + stateDelay);
            }
        }

/*        if (TEMPGLOBALVARIABLES.printTime && (counterEdgesInstance %500) == 0) {
            //ctx.output(GraphPartitionerImpl.outputTag, "REL > " + toBeRemoved.size() + " > " + globalCounterForPrint);
            System.out.println("ELE$" + counterEdgesInstance + "$" + ctx.broadcastWatermark());
        }*/

        if(TEMPGLOBALVARIABLES.printTime) {
            if (counterEdgesInstance < 2) {
                ctx.output(GraphPartitionerImpl.outputTag, "new Job started");
                ctx.output(GraphPartitionerImpl.outputTag,"REP > CurrentWatermark > ratioRepeats > totalNumRepetitions > keysInRepeatMap > keyDistributionRepeat");
            }
            //System.out.println("new Job started");
            if (counterEdgesInstance % TEMPGLOBALVARIABLES.printModulo == 0) {
                String progress = checkTimer();
                //System.out.println(progress);
                ctx.output(GraphPartitionerImpl.outputTag, progress);
            }
        }
    }

    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Edge<Integer, Long>,Integer>> out) throws Exception {
        //System.out.println("hey leute");
        ProcessStateLong edgeState = state.value();
        System.out.println("timer called with key" + state.value().key + " -- and edges: " + state.value().edgeList.size());

        if (readyToGo.keySet().contains(edgeState.key)) {
            ctx.output(GraphPartitionerImpl.outputTag, "inside now for " + edgeState.key + " with # "  + edgeState.edgeList.size() + " edges ----> all items: " + edgeState.edgeList);
            for (Edge e: edgeState.edgeList)
                out.collect(emitEdge(e,1, System.currentTimeMillis(),ctx));
        } else {
            ctx.timerService().registerEventTimeTimer(edgeState.lastModified + stateDelay/2);
            ctx.output(GraphPartitionerImpl.outputTag,"to be repeated for " + state.value().key + " with " + state.value().edgeList);
        }

/*        List<Edge<Integer, Long>> toBeRemovedState = new ArrayList<>();
        List<Edge<Integer, Long>> toBeAddedToWaitingEdges = new ArrayList<>();
        for (Edge e : edgeState.edgeList) {
            boolean checkInside = checkIfEarlyArrived(e);

            if (checkInside) {
                out.collect(emitEdge(e,1, System.currentTimeMillis(),ctx));
                addedInState++;
                toBeRemovedState.add(e);
                if (addedInState % 10000000 == 0)
                    System.out.println("addedInState: " + addedInState);
                //System.out.println("state$1");
            } else {
                toBeAddedToWaitingEdges.add(e);
            }
        }
        int edgeListSizeBefore = edgeState.edgeList.size();
        edgeState.edgeList.removeAll(toBeRemovedState);

        if (edgeState.edgeList.size() > 0)
            ctx.output(GraphPartitionerImpl.outputTag,ctx.broadcastWatermark() + "$ items left in state " + edgeState.key + "," + edgeState.edgeList.size() + " out of " + edgeListSizeBefore + " --> all items: " + edgeState.edgeList);

        if (waitingEdges.size() > 0) {
            ctx.output(GraphPartitionerImpl.outputTag,ctx.broadcastWatermark() + " $ HALLO waiting edges: " + waitingEdges.size() + " -- " + waitingEdges);
            //ctx.output(GraphPartitionerImpl.outputTag,"waiting edges " + edgeState.key + "--" + waitingEdges.size());
            List<Edge<Integer, Long>> toBeRemoved = new ArrayList<>();
            for (Edge<Integer, Long> e : waitingEdges) {
                if (checkIfEarlyArrived(e)) {
                    out.collect(emitEdge(e,2, System.currentTimeMillis(),ctx));
                    collectedEdges++;
                    //System.out.println("late$1");
                    //waitingEdges.remove(e);
                    toBeRemoved.add(e);
                    totalEdgesInWait++;
                } else {
                    stillNotInside++;
                    if (stillNotInside % 1000 == 0)
                        ctx.output(GraphPartitionerImpl.outputTag,ctx.broadcastWatermark() + "$ still not inside$" + stillNotInside);
                    break;
                }
            }
            //totalEdgesInWait = totalEdgesInWait + toBeRemoved.size();
            //System.out.println("totalEdgesWaiting" + totalEdgesInWait);
            waitingEdges.removeAll(toBeRemoved);
        }

        if(toBeAddedToWaitingEdges.size() > 0) {
            totalRepetitions++;

            if (repeatMap.containsKey(edgeState.key)) {
                    int oldVal = repeatMap.get(edgeState.key);
                    int newVal = oldVal + 1;
                    repeatMap.replace(edgeState.key,oldVal,newVal);
            } else {
                repeatMap.put(edgeState.key, 1);
            }

            if (edgeState.repetition > 5 ) {
                ctx.output(GraphPartitionerImpl.outputTag, ctx.broadcastWatermark() + " $ to be repeated $ " + edgeState.key + " " + edgeState.repetition + " $ total: $ " + toBeAddedToWaitingEdges.size());
            }
            if (edgeState.repetition > 50 ) {
                throw new Exception("edgeState " + edgeState.key + " repeated > 50 times");
            }
            if (totalRepetitions % (TEMPGLOBALVARIABLES.printModulo/5000) == 0) {
                HashMap<Integer, Integer> repeatDistribution = getRepetitionDistribution(repeatMap);
                double ratioRepeats = (double) totalRepetitions/ (double) collectedEdges;
                ctx.output(GraphPartitionerImpl.outputTag,"REP > " + ctx.broadcastWatermark() + " > " + df.format(ratioRepeats) + " > " + totalRepetitions + " >  " + repeatMap.size() + " > " + repeatDistribution);
                ctx.output(GraphPartitionerImpl.outputTag,"REP > " + repeatMap.keySet());
            }
            edgeState.repetition++;
            //waitingEdges.addAll(toBeAddedToWaitingEdges);
            edgeState.edgeList = toBeAddedToWaitingEdges;
            edgeState.lastModified = ctx.broadcastWatermark();
            state.update(edgeState);
            ctx.timerService().registerEventTimeTimer(edgeState.lastModified + stateDelay/2);

            }*/




        /*if (globalCounterForPrint % 20000 == 0)
            ctx.output(GraphPartitionerImpl.outputTag,ctx.broadcastWatermark() + "$ total waiting edges: " + totalEdgesInWait);*/
        // get the state for the key that scheduled the timer
        //CountWithTimestamp result = state.value();

        // check if this is an outdated timer or the latest timer
/*        if (timestamp == result.lastModified + 6000) {
            // emit the state on timeout
            out.collect(new Tuple2<>(new Edge<>(1,2,NullValue.getInstance()),1));
        }*/


    }

    private boolean
    checkIfEarlyArrived(Edge<Integer, Long> currentEdge) {

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
        return "MAT > " + System.currentTimeMillis() + " > "  + counterEdgesInstance + " > "  + difference/1000 + " > s";
    }

    private Tuple2<Edge<Integer, Long>,Integer> emitEdge(Edge<Integer, Long> edge,int place, long currTime, BaseBroadcastProcessFunction.ReadOnlyContext ctx) throws Exception {
        int partitionId = modelBuilder.choosePartition(edge);
        collectedEdges++;
        if (collectedEdges % 100000 == 0)
            //ctx.output(GraphPartitionerImpl.outputTag,ctx.broadcastWatermark() + "$ emitted$1$total$"+collectedEdges + "$" + edge + "$" + place + "$");
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



