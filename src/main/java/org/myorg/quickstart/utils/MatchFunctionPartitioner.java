package org.myorg.quickstart.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.partitioners.GraphPartitionerImpl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

import static java.lang.Math.toIntExact;

public class MatchFunctionPartitioner extends KeyedBroadcastProcessFunction<Integer, Edge<Integer, NullValue>, HashMap<Integer, Integer>, Tuple2<Edge<Integer, NullValue>,Integer>> {

    int countBroadcastsOnWorker = 0;
    int counterEdgesInstance = 0;
    int avgWaitingEdges = 0;
    int totalEdgesInWait = 0;
    long totalTimeBroadcast = 0;
    long totalTimeWaitingEdges = 0;
    long broadcastResetCounter = 1;
    long globalCounterForPrint = 0;
    String algorithm;
    HashMap<Integer, Integer> vertexDegreeMap = new HashMap<>();
    ModelBuilderGelly modelBuilder;
    List<Edge<Integer, NullValue>> waitingEdges;
    long startTime = System.currentTimeMillis();

    public MatchFunctionPartitioner(String algorithm, Integer k, double lambda) {
        this.algorithm = algorithm;
        this.waitingEdges = new ArrayList<>();
        if (algorithm.equals("hdrf"))
            this.modelBuilder = new ModelBuilderGelly(algorithm, vertexDegreeMap, k, lambda);
        if (algorithm.equals("dbh"))
            this.modelBuilder = new ModelBuilderGelly(algorithm, vertexDegreeMap, k);
    }


    // This function is called every time when a broadcast state is processed from the previous phase
    @Override
    public void processBroadcastElement(HashMap<Integer, Integer> broadcastElement, Context ctx, Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {

        long startTime = System.nanoTime();
        globalCounterForPrint++;
        countBroadcastsOnWorker++;

        // Print for debugging
        if (TEMPGLOBALVARIABLES.printPhaseTwo)
            System.out.println("Phase 2: Broadcasting HashMap " + broadcastElement);



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

        //System.out.println("Current VertexDepr Partitioning Table: " + vertexPartition);

        if (TEMPGLOBALVARIABLES.printPhaseTwo) {
            System.out.println(countBroadcastsOnWorker + ": " + vertexDegreeMap);
        }

        /*if(TEMPGLOBALVARIABLES.printTime)
            ctx.output(GraphPartitionerImpl.outputTag,"WAI > " + waitingEdges.size() + " > " + globalCounterForPrint);*/



        long waitEdgesTime = 0;

        if (waitingEdges.size() > 0) {
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
            waitingEdges.removeAll(toBeRemoved);
            /*if (TEMPGLOBALVARIABLES.printTime)
                ctx.output(GraphPartitionerImpl.outputTag,"REL > " + toBeRemoved.size() + " > " + globalCounterForPrint);*/
            long endTime2 = System.nanoTime();
            waitEdgesTime = endTime2 - startTime2;
        }


        if (TEMPGLOBALVARIABLES.printTime) {
            long endTime = System.nanoTime();
        }

        if(TEMPGLOBALVARIABLES.printTime) {

            long endTime = System.nanoTime();
            totalTimeBroadcast = totalTimeBroadcast + (endTime - startTime);
            totalTimeWaitingEdges = totalTimeWaitingEdges + waitEdgesTime;
            totalEdgesInWait = totalEdgesInWait + waitingEdges.size();

            if (countBroadcastsOnWorker % (TEMPGLOBALVARIABLES.printModulo/4) == 0 && countBroadcastsOnWorker > 0) {
                long diff = endTime-startTime;
                long totalBroadcastedCounter = countBroadcastsOnWorker*broadcastResetCounter;
                float avgRatio = ((float) totalTimeWaitingEdges / totalTimeBroadcast);
                long avgTime = totalTimeBroadcast / countBroadcastsOnWorker;
                long avgWaiting = totalEdgesInWait / countBroadcastsOnWorker;
                /*if (avgWaiting < 0)
                    System.out.println(" -- minus : edgesWaiting" + totalEdgesInWait + " --- broadcastCounter " + countBroadcastsOnWorker);*/

                //System.out.println("BRO > " + globalCounterForPrint  + " > " + avgTime + " > " + avgRatio + " > " + avgWaiting);
                ctx.output(GraphPartitionerImpl.outputTag,"BRO > " + globalCounterForPrint  + " > " + avgTime + " > " + avgRatio + " > " + avgWaiting);
                totalTimeWaitingEdges = 0;
                totalTimeBroadcast = 0;
                totalEdgesInWait = 0;
                countBroadcastsOnWorker = 0;
                broadcastResetCounter++;
            }



        }

        //System.out.println("That took " + (endTime - startTime) + " milliseconds");

    }

    @Override
    public void processElement(Edge<Integer, NullValue> currentEdge, ReadOnlyContext ctx, Collector<Tuple2<Edge<Integer, NullValue>,Integer>> out) throws Exception {

        //System.out.println("inside Process: Edge (" + currentEdge.getEdge().f0 + " " + currentEdge.getEdge().f1 + "): " + currentEdge.getEdge().f0.getClass() + " " + currentEdge.getEdge().f1.getClass() + " -- ");
        boolean checkInside = checkIfEarlyArrived(currentEdge);

        if (!checkInside) {
            //System.out.println("EdgeDepr (" + currentEdge.getEdge().f0 + " " + currentEdge.getEdge().f1 + ") not inside. added to queue");
            waitingEdges.add(currentEdge);
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

        //int partitionId = modelBuilder.choosePartition(currentEdge);
        //System.out.println("Phase 2: " + currentEdge.getEdge().getOriginVertexDepr() + " " + currentEdge.getEdge().getDestinVertexDepr() + " --> " + partitionId);

        // Add to "SINK" (TODO: Real Sink Function)
        //edgeInPartition.put(currentEdge,partitionId);

        //if (PhasePartitionerGelly.printPhaseTwo == true) {
            //System.out.println(checkInside);

        //out.collect(new Tuple2<>(currentEdge,partitionId));

        //ctx.output(GraphPartitionerImpl.outputTag, "1: " + modelBuilder.getHdrf().getCurrentState().printState().toString());

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



