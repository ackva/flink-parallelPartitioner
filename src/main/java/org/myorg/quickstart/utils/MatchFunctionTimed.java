package org.myorg.quickstart.utils;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.co.BaseBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.partitioners.GraphPartitionerImpl;

import java.util.*;

import static java.lang.Math.toIntExact;


public class MatchFunctionTimed extends KeyedBroadcastProcessFunction<Integer, Edge<Integer, NullValue>, HashMap<Integer, Integer>, Tuple2<Edge<Integer, NullValue>,Integer>> {

    private int totalRepetitions = 0;
    private int collectedEdges = 0;
    private int stillNotInside = 0;
    private HashMap<String,Long> outputEdges = new HashMap<>();
    private List<Edge> duplicates = new ArrayList<>();
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
    private ModelBuilderGelly modelBuilder;
    private List<Edge<Integer, NullValue>> waitingEdges;
    private long startTime = System.currentTimeMillis();
    private int stateDelay = 0;
    /** The state that is maintained by this process function */
    private ValueState<ProcessState> state;
   // private ListState<ProcessState> state1;


    public MatchFunctionTimed(String algorithm, Integer k, double lambda, int stateDelay) {
        this.algorithm = algorithm;
        this.stateDelay = stateDelay;
        this.waitingEdges = new ArrayList<>();
        if (algorithm.equals("hdrf"))
            this.modelBuilder = new ModelBuilderGelly(algorithm, vertexDegreeMap, k, lambda);
        if (algorithm.equals("dbh"))
            this.modelBuilder = new ModelBuilderGelly(algorithm, vertexDegreeMap, k);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", ProcessState.class));
        //System.out.println("state delay: " + stateDelay);
    }

    // This function is called every time when a broadcast state is processed from the previous phase
    @Override
    public void processBroadcastElement(HashMap<Integer, Integer> broadcastElement, Context ctx, Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {

        //ctx.output(GraphPartitionerImpl.outputTag,ctx.currentWatermark() + "$" + broadcastElement + " ####### ALIVE ######");
        globalCounterForPrint++;
        countBroadcastsOnWorker++;

        if (this.algorithm.equals("hdrf")) {
            // ### Merge local model from Phase 1 with global model, here in Phase 2
            Iterator it = broadcastElement.entrySet().iterator();
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
            Iterator it = broadcastElement.entrySet().iterator();
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


/*        ProcessState testState = state.value();
        if (testState != null)
            ctx.output(GraphPartitionerImpl.outputTag,ctx.currentWatermark() + "$" + testState.key + " -- hello from broadie");*/



    }

    @Override
    public void processElement(Edge<Integer, NullValue> currentEdge, ReadOnlyContext ctx, Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {

        //System.out.println("Edge in Match(" + currentEdge + " $ " + ctx.timestamp() + " $ current watermark:  $" + ctx.currentWatermark());

        //System.out.println("inside Process: Edge (" + currentEdge.getEdge().f0 + " " + currentEdge.getEdge().f1 + "): " + currentEdge.getEdge().f0.getClass() + " " + currentEdge.getEdge().f1.getClass() + " -- ");

        //System.out.println("3$" + ctx.currentWatermark() + "$" + ctx.currentProcessingTime() + "$" + ctx.timerService().currentProcessingTime() + "$" + currentEdge);
        counterEdgesInstance++;

        //boolean checkInside = checkIfEarlyArrived(currentEdge);

/*        if (checkInside) {
            out.collect(emitEdge(currentEdge,0));
            addedDirectly++;
            if (addedDirectly % 10000000 == 0)
                System.out.println("addedDirectly: " + addedDirectly);
            //System.out.println("direct$1");
        } else {*/
            // retrieve the current count
            ProcessState current = state.value();
            if (current == null) {
                //System.out.println("new state created" + currentEdge);
                current = new ProcessState();
                current.key = currentEdge;
            } else {
                //System.out.println("state exisits " + state.value().edgeList.get(0) + " -- this is current " + currentEdge);
            }

            // update the state's count
            current.edgeList.add(currentEdge);
        //System.out.println(current.key.f0 + " .. size " + current.edgeList.size());
            //System.out.println(current.key + " - " + current.edge);

            // set the state's timestamp to the record's assigned event time timestamp
            current.lastModified = ctx.currentWatermark();

            // write the state back
            state.update(current);

            // schedule the next timer X seconds from the current event time
            ctx.timerService().registerEventTimeTimer(current.lastModified + (stateDelay));
        //System.out.println(current.key + " registered for " + (current.lastModified + 600));
            //ctx.timerService().registerEventTimeTimer(current.lastModified);

        //}


/*        if(TEMPGLOBALVARIABLES.printTime) {
            if (counterEdgesInstance < 2)
                ctx.output(GraphPartitionerImpl.outputTag, "new Job started");
                //System.out.println("new Job started");
            if (counterEdgesInstance % TEMPGLOBALVARIABLES.printModulo == 0) {
                String progress = checkTimer();
                //System.out.println(progress);
                ctx.output(GraphPartitionerImpl.outputTag, progress);
            }
        }*/

/*        if (TEMPGLOBALVARIABLES.printTime && (counterEdgesInstance %500) == 0) {
            //ctx.output(GraphPartitionerImpl.outputTag, "REL > " + toBeRemoved.size() + " > " + globalCounterForPrint);
            System.out.println("ELE$" + counterEdgesInstance + "$" + ctx.currentWatermark());
        }*/

    }

    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {
        //System.out.println("hey leute");
        ProcessState edgeState = state.value();

        /*if (edgeState.repetition > 0) {
            boolean sourceInside = false;
            boolean targetInside = false;
            sourceInside = modelBuilder.getHdrf().getCurrentState().checkIfRecordExits(Long.parseLong(edgeState.key.f0.toString()));
            targetInside = modelBuilder.getHdrf().getCurrentState().checkIfRecordExits(Long.parseLong(edgeState.key.f1.toString()));
            if (sourceInside && targetInside)
                ctx.output(GraphPartitionerImpl.outputTag,edgeState.key + " inside now");
            if (!targetInside && sourceInside)
                ctx.output(GraphPartitionerImpl.outputTag,edgeState.key.f1 + " still not inside state");
            if (targetInside && !sourceInside)
                ctx.output(GraphPartitionerImpl.outputTag,edgeState.key.f0 + " still not inside state");
            if (!targetInside && !sourceInside)
                ctx.output(GraphPartitionerImpl.outputTag,edgeState.key + " still not inside state");
        }*/

        //onTimerCount++;
        //onTimerCount=onTimerCount+edgeState.edgeList.size();
        //System.out.println("onTimer$"+onTimerCount);
        //ctx.output(GraphPartitionerImpl.outputTag,"onTimer$"+onTimerCount);
        //System.out.println(onTimerCount + " " + state.value().edgeList.size() + " " + state.value().key + " " + timestamp + " " + ctx.timerService().currentWatermark() + " " + System.currentTimeMillis());

        //System.out.println("3$" + ctx.currentWatermark() + "$" + ctx.currentProcessingTime() + "$" + ctx.timerService().currentProcessingTime() + "$");
        //System.out.println("onTimerCount$1");

        //System.out.println("waiting edges in onTime call: " + result.stateWaitList.size() + " ... waitingEdges local list " + waitingEdges.size());

        List<Edge<Integer, NullValue>> toBeRemovedState = new ArrayList<>();
        List<Edge<Integer, NullValue>> toBeAddedToWaitingEdges = new ArrayList<>();
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
            //ctx.output(GraphPartitionerImpl.outputTag,ctx.currentWatermark() + "$ items left in state " + edgeState.key.f0 + "," + edgeState.key.f1 + " - " + edgeState.edgeList.size() + " out of " + edgeListSizeBefore + " --> all items: " + edgeState.edgeList);

        if (waitingEdges.size() > 0) {
            ctx.output(GraphPartitionerImpl.outputTag,ctx.currentWatermark() + " $ HALLO waiting edges: " + waitingEdges.size() + " -- " + waitingEdges);
            //ctx.output(GraphPartitionerImpl.outputTag,"waiting edges " + edgeState.key + "--" + waitingEdges.size());
            List<Edge<Integer, NullValue>> toBeRemoved = new ArrayList<>();
            for (Edge<Integer, NullValue> e : waitingEdges) {
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
                        ctx.output(GraphPartitionerImpl.outputTag,ctx.currentWatermark() + "$ still not inside$" + stillNotInside);
                    break;
                }
            }
            //totalEdgesInWait = totalEdgesInWait + toBeRemoved.size();
            //System.out.println("totalEdgesWaiting" + totalEdgesInWait);
            waitingEdges.removeAll(toBeRemoved);
        }

        if(toBeAddedToWaitingEdges.size() > 0) {
            totalRepetitions++;
            if (totalRepetitions % 2000 == 0)
                ctx.output(GraphPartitionerImpl.outputTag,ctx.currentWatermark() + " repetitions $ " + totalRepetitions);
            if (edgeState.repetition > 5 ) {
                ctx.output(GraphPartitionerImpl.outputTag, ctx.currentWatermark() + " $ to be repeated $ " + edgeState.key + " " + edgeState.repetition + " $ total: $ " + toBeAddedToWaitingEdges.size());
            }
            if (edgeState.repetition > 5000 ) {
                throw new Exception("FATAL ERROR");
            }
            edgeState.repetition++;
            //waitingEdges.addAll(toBeAddedToWaitingEdges);
            edgeState.edgeList = toBeAddedToWaitingEdges;
            edgeState.lastModified = ctx.currentWatermark();
            state.update(edgeState);
            ctx.timerService().registerEventTimeTimer(edgeState.lastModified + stateDelay/2);
        }


        /*if (globalCounterForPrint % 20000 == 0)
            ctx.output(GraphPartitionerImpl.outputTag,ctx.currentWatermark() + "$ total waiting edges: " + totalEdgesInWait);*/
        // get the state for the key that scheduled the timer
        //CountWithTimestamp result = state.value();

        // check if this is an outdated timer or the latest timer
/*        if (timestamp == result.lastModified + 6000) {
            // emit the state on timeout
            out.collect(new Tuple2<>(new Edge<>(1,2,NullValue.getInstance()),1));
        }*/


    }

    private boolean
    checkIfEarlyArrived(Edge<Integer, NullValue> currentEdge) {

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
        return "MAT > " + counterEdgesInstance + " > "  + difference/1000 + " > s";
    }

    private Tuple2<Edge<Integer, NullValue>,Integer> emitEdge(Edge<Integer, NullValue> edge,int place, long currTime, BaseBroadcastProcessFunction.ReadOnlyContext ctx) throws Exception {
        int partitionId = modelBuilder.choosePartition(edge);
        collectedEdges++;
        if (collectedEdges % 100000 == 0)
            //ctx.output(GraphPartitionerImpl.outputTag,ctx.currentWatermark() + "$ emitted$1$total$"+collectedEdges + "$" + edge + "$" + place + "$");
        if (place != 1)
            //ctx.output(GraphPartitionerImpl.outputTag,"WAITING__$1$total$"+collectedEdges + "$" + edge + "$" + place + "$$");
        if (outputEdges.containsKey(edge.toString())) {
            //ctx.output(GraphPartitionerImpl.outputTag,ctx.currentWatermark() + "$ " + edge + " -- " + duplicates.size() + " duplicate (delayed by) " + (outputEdges.get(edge.toString())-currTime));
            duplicates.add(edge);

            }
        outputEdges.put(edge.toString(),currTime);
        //System.out.println(collectedEdges);
        return new Tuple2<>(edge, partitionId);
    }

}



