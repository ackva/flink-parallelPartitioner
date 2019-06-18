package org.myorg.quickstart.utils;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.partitioners.GraphPartitionerImpl;
import org.myorg.quickstart.testsigma.CountWithTimestamp;

import java.util.*;

import static java.lang.Math.toIntExact;
public class MatchFunctionTimed extends KeyedBroadcastProcessFunction<Integer, Edge<Integer, NullValue>, HashMap<Integer, Integer>, Tuple2<Edge<Integer, NullValue>,Integer>> {
//public class MatchFunctionTimed extends KeyedBroadcastProcessFunctionCustom<Integer, Edge<Integer, NullValue>, HashMap<Integer, Integer>, Tuple2<Edge<Integer, NullValue>,Integer>> {

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
    /** The state that is maintained by this process function */
    private ValueState<CountWithTimestamp> state;

    public MatchFunctionTimed(String algorithm, Integer k, double lambda) {
        this.algorithm = algorithm;
        this.waitingEdges = new ArrayList<>();
        if (algorithm.equals("hdrf"))
            this.modelBuilder = new ModelBuilderGelly(algorithm, vertexDegreeMap, k, lambda);
        if (algorithm.equals("dbh"))
            this.modelBuilder = new ModelBuilderGelly(algorithm, vertexDegreeMap, k);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
    }

    // This function is called every time when a broadcast state is processed from the previous phase
    @Override
    public void processBroadcastElement(HashMap<Integer, Integer> broadcastElement, Context ctx, Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {

        long startTime = System.nanoTime();
        globalCounterForPrint++;
        countBroadcastsOnWorker++;

        //System.out.println("Phase 2: Broadcasting HashMap " + waitingEdges.size() + " -- " + broadcastElement + " $ " + ctx.timestamp() + " $  current watermark: $ " + ctx.currentWatermark());


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



/*            if (TEMPGLOBALVARIABLES.printTime && (globalCounterForPrint %500) == 0) {
                //ctx.output(GraphPartitionerImpl.outputTag, "REL > " + toBeRemoved.size() + " > " + globalCounterForPrint);
                System.out.println("BRO$" + globalCounterForPrint + "$" + ctx.currentWatermark());
                long endTime2 = System.nanoTime();
            }*/


/*        if (TEMPGLOBALVARIABLES.printPhaseTwo) {
            System.out.println(countBroadcastsOnWorker + ": " + vertexDegreeMap);
        }*/


       // long waitEdgesTime = 0;



    }

    @Override
    public void processElement(Edge<Integer, NullValue> currentEdge, ReadOnlyContext ctx, Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {

        //System.out.println("Edge in Match(" + currentEdge + " $ " + ctx.timestamp() + " $ current watermark:  $" + ctx.currentWatermark());

        //System.out.println("inside Process: Edge (" + currentEdge.getEdge().f0 + " " + currentEdge.getEdge().f1 + "): " + currentEdge.getEdge().f0.getClass() + " " + currentEdge.getEdge().f1.getClass() + " -- ");
        boolean checkInside = checkIfEarlyArrived(currentEdge);
//

        // retrieve the current count
        CountWithTimestamp current = state.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.key = currentEdge.f0.toString();
        }

        // update the state's count
        current.count++;
       // System.out.println(current.key + " - " + current.count);

        // set the state's timestamp to the record's assigned event time timestamp
        current.lastModified = ctx.timestamp();

        // write the state back
        state.update(current);

        // schedule the next timer 60 seconds from the current event time
        ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);



        if (!checkInside) {
            //System.out.println("EdgeDepr (" + currentEdge.getEdge().f0 + " " + currentEdge.getEdge().f1 + ") not inside. added to queue");
            waitingEdges.add(currentEdge);

/*          int partitionId = MathUtils.murmurHash(currentEdge.f0.hashCode()) % GraphPartitionerImpl.k;
            out.collect(new Tuple2<>(currentEdge, partitionId));*/

        } else {
            int partitionId = modelBuilder.choosePartition(currentEdge);
            out.collect(new Tuple2<>(currentEdge, partitionId));
        }


        counterEdgesInstance++;

        if(TEMPGLOBALVARIABLES.printTime) {
            if (counterEdgesInstance < 2)
                ctx.output(GraphPartitionerImpl.outputTag, "new Job started");
                //System.out.println("new Job started");
            if (counterEdgesInstance % TEMPGLOBALVARIABLES.printModulo == 0) {
                String progress = checkTimer();
                //System.out.println(progress);
                ctx.output(GraphPartitionerImpl.outputTag, progress);
            }
        }

/*        if (TEMPGLOBALVARIABLES.printTime && (counterEdgesInstance %500) == 0) {
            //ctx.output(GraphPartitionerImpl.outputTag, "REL > " + toBeRemoved.size() + " > " + globalCounterForPrint);
            System.out.println("ELE$" + counterEdgesInstance + "$" + ctx.currentWatermark());
        }*/

    }

    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {

        //System.out.println("hey leute");
        // get the state for the key that scheduled the timer
        CountWithTimestamp result = state.value();

        // check if this is an outdated timer or the latest timer
/*        if (timestamp == result.lastModified + 6000) {
            // emit the state on timeout
            out.collect(new Tuple2<>(new Edge<>(1,2,NullValue.getInstance()),1));
        }*/

        if (waitingEdges.size() > 0) {
            System.out.println("calling this");
            long startTime2 = System.nanoTime();
            List<Edge<Integer, NullValue>> toBeRemoved = new ArrayList<>();
            for (Edge<Integer, NullValue> e : waitingEdges) {
                if (checkIfEarlyArrived(e)) {
                    int partitionId = modelBuilder.choosePartition(e);
                    out.collect(new Tuple2<>(e, partitionId));
                    toBeRemoved.add(e);
                } else {
                    break;
                }
            }
            totalEdgesInWait = totalEdgesInWait + toBeRemoved.size();
            System.out.println("totalEdgesWaiting" + totalEdgesInWait);
            waitingEdges.removeAll(toBeRemoved);
        }

        if (globalCounterForPrint % 20 == 0)
            ctx.output(GraphPartitionerImpl.outputTag,"total waiting edges: " + totalEdgesInWait);
    }

    private boolean checkIfEarlyArrived(Edge<Integer, NullValue> currentEdge) {

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

}



