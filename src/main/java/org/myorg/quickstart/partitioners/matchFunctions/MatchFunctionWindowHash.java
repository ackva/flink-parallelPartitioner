package org.myorg.quickstart.partitioners.matchFunctions;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.co.BaseBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.partitioners.GraphPartitionerImpl;
import org.myorg.quickstart.partitioners.GraphPartitionerMultipleTemp;
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

public class MatchFunctionWindowHash extends KeyedBroadcastProcessFunction<Integer, Edge<Integer, Long>, Tuple2<HashMap<Integer, Integer>,Long>, Tuple2<Edge<Integer, Long>,Integer>> {

    long currentWatermarkBro = 1;
    long currentWatermarkEle = 1;
    private HashMap<Long,Integer> repeatMap = new HashMap<>();
    private HashMap<ProcessStateLong,Long> allStates = new HashMap<>();
    private List<ProcessStateLong> allStateList = new ArrayList<>();


   /* MapStateDescriptor<Long, Boolean> stateDescriptor = new MapStateDescriptor<>
            ("TestBroadCastState", BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(new TypeHint<Boolean>() {}));*/

/*    private final MapStateDescriptor<Long, Boolean> readyToGoStateDescriptor =
            new MapStateDescriptor<>(
                    "RulesBroadcastState",
                    BasicTypeInfo.LONG_TYPE_INFO,
                    TypeInformation.of(new TypeHint<Boolean>() {}));*/

    boolean lastcall;
    private int stateCounter;
    long broadcastWatermark = 1;
    List<Long> watermarksBro = new ArrayList<>();
    List<Long> watermarksEle = new ArrayList<>();
    private HashMap<Long,Boolean> readyToGo = new HashMap<>();
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

    // private ListState<ProcessState> state1;
   //MapStateDescriptor<String, Tuple2<Long,Boolean>> updatedStateDescriptor = new MapStateDescriptor<>("keineAhnung", BasicTypeInfo.STRING_TYPE_INFO, ValueTypeInfo.BOOLEAN_VALUE_TYPE_INFO);
    private int nullCounter = 0;


    public MatchFunctionWindowHash(String algorithm, Integer k, double lambda, int stateDelay) {
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

        globalCounterForPrint++;
        countBroadcastsOnWorker++;

        int numEdgesInBroadcast = 0;
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
                numEdgesInBroadcast += stateEntry.getValue();
        }


        checkDifference(broadcastElement.f1, 0,ctx.currentWatermark(),ctx.currentProcessingTime(), numEdgesInBroadcast, broadcastElement.f0.toString());


        numEdgesInBroadcast *= 0.5;
        //edgesInBroadcast = numEdgesInBroadcast / 4;

        ctx.output(GraphPartitionerImpl.outputTag,"BROAD > " + broadcastElement.f1 + " > " + numEdgesInBroadcast +  " > " + broadcastElement.f0);
        checkDifference(broadcastElement.f1, 0,ctx.currentWatermark(),ctx.currentProcessingTime(), numEdgesInBroadcast, broadcastElement.f0.toString());

        readyToGo.put(broadcastElement.f1,true);
        //System.out.println(broadcastElement.f1 + " now fully added to state");

    }

    @Override
    public void processElement(Edge<Integer, Long> currentEdge, ReadOnlyContext ctx, Collector<Tuple2<Edge<Integer, Long>,Integer>> out) throws Exception {

        counterEdgesInstance++;

        checkDifference(currentEdge.f2,1,ctx.currentWatermark(),ctx.currentProcessingTime(), 1, currentEdge.toString());

        if (currentEdge.f2 == -7229148735837822083L)
            System.out.println(currentEdge + " has f2 value -7229148735837822083");


/*
        ##### NEUEN "State" wie waiting edges. deutlich schneller dann :)
        if (in state)
            assignEdges (edgeList)
*/


        ProcessStateLong current = state.value();
            if (current == null) {
                current = new ProcessStateLong();
                current.key = currentEdge.f2;

                stateCounter++;
                ctx.output(GraphPartitionerImpl.outputTag,"new state created for edge " + currentEdge + " -- state key: " + current.key);
                //allStateList.add(current);
                //allStates.put(current,current.key);
            } else {

                //System.out.println("state exists " + state.value().edgeList.get(0) + " -- this is current " + currentEdge);
            }

            // add edge to state list
            current.edgeList.add(currentEdge);

             // set the state's timestamp to the record's assigned event time timestamp
             current.lastModified = ctx.currentProcessingTime();

            // write the state back
            state.update(current);

            // schedule the next timer X milliseconds from the current event time
            ctx.timerService().registerEventTimeTimer(current.lastModified + (stateDelay));

/*        if (!state.value().key.equals(currentEdge.f2)) {
                    System.out.println("das ist ja dumm" + state.value().key + " -- " + state.value().edgeList.get(0) + " -- edge :" + currentEdge.f0 + "," + currentEdge.f1 + " .. " + currentEdge.f2);
                    //#### HIER MUSS MAN EINE LÖSUNG IMPLEMENTIEREN; DIE "PRO WINDOW" ARbEITET!!!

                } else {
                    //System.out.println("adding edge " + currentEdge + " to state " + current.key + " with e.g. " + current.edgeList.get(0));
                    current.edgeList.add(currentEdge);
                }*/

                /*try {
                    ctx.output(GraphPartitionerImpl.outputTag,"state " + state.value().key + " exists, with " + state.value().edgeList.get(0) + " -- this is current edge " + currentEdge);
                    //ctx.output(GraphPartitionerImpl.outputTag,"state " + state.value().key + " exists, with " + state.value().key + " -- this is current edge " + currentEdge);
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("state " + state.value() + " exists, but no edgeList?! " + currentEdge + " state key " + state.value().key);
                    System.out.println("number of states: " + stateCounter);
                    //System.out.println("all states: " + allStates);
                }*/

            // update the state's count
            //current.edgeList.add(currentEdge);
            //ctx.output(GraphPartitionerImpl.outputTag,"State " + current.key + ", size " + current.edgeList.size() + ", with:" + current.edgeList);
            //ctx.output(GraphPartitionerImpl.outputTag,current.key + " - " + current.key);





        if(TEMPGLOBALVARIABLES.printTime) {
            if (counterEdgesInstance < 2) {
                ctx.output(GraphPartitionerImpl.outputTag, "new Job started");
                ctx.output(GraphPartitionerImpl.outputTag,"REP > CurrentWatermark > ratioRepeats > totalNumRepetitions > keysInRepeatMap > keyDistributionRepeat");
            }
            if (counterEdgesInstance % TEMPGLOBALVARIABLES.printModulo == 0) {
                String progress = checkTimer();
                ctx.output(GraphPartitionerImpl.outputTag, progress);
            }
            if (counterEdgesInstance % 100000 == 0) {
                String progress = checkTimer();
                System.out.println(progress);
            }
        }
    }

    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Edge<Integer, Long>,Integer>> out) throws Exception {

        //System.out.println("hey leute");
        ProcessStateLong edgeState = state.value();
        onTimerCount++;
        ctx.output(GraphPartitionerImpl.outputTag,"timer called with key " + state.value().key + " -- and edges: " + state.value().edgeList.size());

        if (onTimerCount < 2)
            ctx.output(GraphPartitionerImpl.outputTag,"TIME > states/totalCount > repetitions/totalCount > nullCount/totalCount");

        if (onTimerCount % TEMPGLOBALVARIABLES.printModulo == 0)
            ctx.output(GraphPartitionerImpl.outputTag,"TIME > " + df.format((double) stateCounter / (double) onTimerCount) + " > " + df.format((double) totalRepetitions / (double) onTimerCount)+ " > " + df.format((double) totalRepetitions / (double) onTimerCount) + " (total: " + onTimerCount + " timer calls)");

        //ctx.output(GraphPartitionerImpl.outputTag,onTimerCount + " total timer calls");


        if (state.value().edgeList.size() == 0) {
            //System.out.println("timer called with key " + state.value().key + " -- and edges: " + state.value().edgeList.size());
            nullCounter++;
            ctx.timerService().deleteEventTimeTimer(edgeState.lastModified + stateDelay);
            if (nullCounter % 1000 == 0)
                ctx.output(GraphPartitionerImpl.outputTag,nullCounter + " total nullTimer calls");
            return;
        }

        if (state.value().repetition > 0 ) {
            ctx.output(GraphPartitionerImpl.outputTag, edgeState.key + " state called (" + state.value().repetition + "x before) with " + state.value().edgeList.size() + " edges waiting - " + (ctx.currentProcessingTime() - state.value().lastModified) + " - now: " + ctx.currentProcessingTime());
            //ctx.output(GraphPartitionerImpl.outputTag,edgeState.key + " state edge list: " + edgeState.edgeList + "");
        }

        List<Edge<Integer, Long>> toBeRemovedState = new ArrayList<>();
        List<Edge<Integer, Long>> toBeAddedToWaitingEdges = new ArrayList<>();

        if (readyToGo.keySet().contains(edgeState.key)) {
            int sizeBefore = edgeState.edgeList.size();

            //ctx.output(GraphPartitionerImpl.outputTag,"now inside " + edgeState.key + " with " +  edgeState.edgeList.size() + " edges");

            for (Edge e : edgeState.edgeList) {
                boolean checkInside = checkIfEarlyArrived(e);

                if (checkInside) {
                    out.collect(emitEdge(e, 1, System.currentTimeMillis(), ctx));
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
            if (edgeState.edgeList.size() == edgeListSizeBefore && edgeListSizeBefore > 0)
                ctx.output(GraphPartitionerMultipleTemp.outputTagError,"NO IMPACT!! edge List now as big as before: " + edgeState.edgeList.size() + " edges for key " + edgeState.key);


            if (state.value().edgeList.size() == 0) {
                //System.out.println("ja, NULL");
                ctx.timerService().deleteEventTimeTimer(edgeState.lastModified + stateDelay);
            }


            if(toBeAddedToWaitingEdges.size() > 0) {
                ctx.output(GraphPartitionerMultipleTemp.outputTagError,"This is the state now - " + modelBuilder.getHdrf().getCurrentState().getRecord_map());
                ctx.output(GraphPartitionerMultipleTemp.outputTagError,"Could not find - " + toBeAddedToWaitingEdges);
                ctx.output(GraphPartitionerMultipleTemp.outputTagError,"But could find - " + toBeRemovedState);

                totalRepetitions++;

                if (repeatMap.containsKey(edgeState.key)) {
                    int oldVal = repeatMap.get(edgeState.key);
                    int newVal = oldVal + 1;
                    repeatMap.replace(edgeState.key,oldVal,newVal);
                } else {
                    repeatMap.put(edgeState.key, 1);
                }

                if (edgeState.repetition > 0 ) {
                    ctx.output(GraphPartitionerMultipleTemp.outputTagError, ctx.currentProcessingTime() + " $ to be repeated $ " + edgeState.key + " " + edgeState.repetition + " $ total: $ " + toBeAddedToWaitingEdges.size());
                }
                if (edgeState.repetition > 50 ) {
                    throw new Exception("edgeState " + edgeState.key + " repeated > 50 times");
                }
                if (totalRepetitions % (TEMPGLOBALVARIABLES.printModulo) == 0) {
                    HashMap<Integer, Integer> repeatDistribution = getRepetitionDistribution(repeatMap);
                    double ratioRepeats = (double) totalRepetitions/ (double) collectedEdges;
                    ctx.output(GraphPartitionerImpl.outputTag,"REP > " + ctx.currentWatermark() + " > " + df.format(ratioRepeats) + " > " + totalRepetitions + " >  " + repeatMap.size() + " > " + repeatDistribution);
                    ctx.output(GraphPartitionerImpl.outputTag,"REP > " + repeatMap.keySet());
                }
                edgeState.repetition++;
                //waitingEdges.addAll(toBeAddedToWaitingEdges);
                edgeState.edgeList = toBeAddedToWaitingEdges;
                edgeState.lastModified = ctx.currentWatermark();
                state.update(edgeState);
                ctx.timerService().registerEventTimeTimer(edgeState.lastModified + stateDelay);

                }

        /*if (edgeState.edgeList.size() > 0)
            ctx.output(GraphPartitionerImpl.outputTag,ctx.broadcastWatermark() + "$ items left in state " + edgeState.key + "," + edgeState.edgeList.size() + " out of " + edgeListSizeBefore + " --> all items: " + edgeState.edgeList);*/



            } else {
            edgeState.repetition++;
            //waitingEdges.addAll(toBeAddedToWaitingEdges);
            edgeState.edgeList = toBeAddedToWaitingEdges;
            edgeState.lastModified = ctx.currentProcessingTime();
            state.update(edgeState);
            ctx.timerService().registerEventTimeTimer(edgeState.lastModified + stateDelay*2);
        }




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

    private HashMap<Long, EdgeArrivalChecker> edgeArrivedCheckerMap = new HashMap<>();
    private int completeCounter = 0;

    //private HashMap<Long, Integer> arrivedElemDiffSysTime = new HashMap<>();

    private void checkDifference(long hashvalue, int sourceFunction, long watermark, long processingTime, int edgeCounter, String currentElement) throws Exception {

        if (watermark > 999999999 && !lastcall) {
            lastcall = true;
            //System.out.println("last watermark !!!!!!!");
            for( EdgeArrivalChecker eac : edgeArrivedCheckerMap.values() ) {
                if (!eac.isComplete()) {
                    //System.out.println(eac.getHashValue() + " not complete. Still to do: " + eac.getCounterEdgesElements() + " / " + eac.getCounterEdgesBroadcast());
                }
            }
        }


        if (edgeArrivedCheckerMap.containsKey(hashvalue)) {
            EdgeArrivalChecker edgeArrival = edgeArrivedCheckerMap.get(hashvalue);
            edgeArrival.updateDifferences(sourceFunction,watermark,processingTime,edgeCounter, currentElement);
            List<Integer> callHistory = edgeArrival.getCallHistory();
            if (edgeArrival.getCounterEdgesBroadcast() == edgeArrival.getCounterEdgesElements()) {
                edgeArrival.isComplete();
                completeCounter++;
                //if (completeCounter % 100 == 0)
                    //System.out.println(hashvalue + " set is complete with " + edgeArrival.getCounterEdgesElements() + " vs " + edgeArrival.getCounterEdgesBroadcast() + " --- total: " + completeCounter + " out of " + edgeArrivedCheckerMap.size());

                //System.out.println("complete: " + completeCounter + " out of " + edgeArrivedCheckerMap.size());
            }
            //System.out.println(hashvalue + ": " + callHistory);
            //avgDiffWatermark = edgeArrival.get(hashvalue) / edgeCounter;
            //avgDiffWatermark = arrivedDiffProcessing.get(hashvalue) / edgeCounter;
        } else {
            EdgeArrivalChecker edgeArrival = new EdgeArrivalChecker(hashvalue,sourceFunction,watermark,processingTime,edgeCounter, currentElement);
            edgeArrivedCheckerMap.put(hashvalue,edgeArrival);
            edgeArrival.printCreateMessage();

        }





    }

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
        return "MAT > " + System.currentTimeMillis() + " > "  + counterEdgesInstance + " > "  + difference/1000 + " > s";
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



