package org.myorg.quickstart.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.*;

import static java.lang.Math.toIntExact;

public class MatchFunctionPartitioner extends KeyedBroadcastProcessFunction<Integer, Edge<Long, NullValue>, HashMap<Long, Long>, Tuple2<Edge<Long, NullValue>,Integer>> {

    int countBroadcastsOnWorker = 0;
    int counterEdgesInstance = 0;
    String algorithm;
    HashMap<Long, Long> vertexDegreeMap = new HashMap<>();
    ModelBuilderGelly modelBuilder;
    List<Edge<Long, NullValue>> waitingEdges;

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
    public void processBroadcastElement(HashMap<Long, Long> broadcastElement, Context ctx, Collector<Tuple2<Edge<Long, NullValue>,Integer>> out) throws Exception {

        countBroadcastsOnWorker++;

        // Print for debugging
        if (TEMPGLOBALVARIABLES.printPhaseTwo)
            System.out.println("Phase 2: Broadcasting HashMap " + broadcastElement);

        if (this.algorithm.equals("hdrf")) {
            // ### Merge local model from Phase 1 with global model, here in Phase 2
            Iterator it = broadcastElement.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, Long> stateEntry = (Map.Entry)it.next();
                if(modelBuilder.getHdrf().getCurrentState().checkIfRecordExits(stateEntry.getKey())) {
                    //System.out.println("entry " + stateEntry + " exists: degree before: " + modelBuilder.getHdrf().getCurrentState().getRecord(stateEntry.getKey()).getDegree());
                    int degree = toIntExact(stateEntry.getValue()) + modelBuilder.getHdrf().getCurrentState().getRecord(stateEntry.getKey()).getDegree();
                    modelBuilder.getHdrf().getCurrentState().getRecord(stateEntry.getKey()).setDegree(degree);
                } else {
                    modelBuilder.getHdrf().getCurrentState().addRecordWithDegree(stateEntry.getKey(), toIntExact(stateEntry.getValue()));
                }
            }
        }

        if (this.algorithm.equals("dbh")) {
            // ### Merge local model from Phase 1 with global model, here in Phase 2
            Iterator it = broadcastElement.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, Long> stateEntry = (Map.Entry)it.next();
                if(modelBuilder.getDbh().getCurrentState().checkIfRecordExits(stateEntry.getKey())) {
                    //System.out.println("entry " + stateEntry + " exists: degree before: " + modelBuilder.getHdrf().getCurrentState().getRecord(stateEntry.getKey()).getDegree());
                    int degree = toIntExact(stateEntry.getValue()) + modelBuilder.getDbh().getCurrentState().getRecord(stateEntry.getKey()).getDegree();
                    modelBuilder.getDbh().getCurrentState().getRecord(stateEntry.getKey()).setDegree(degree);
                } else {
                    modelBuilder.getDbh().getCurrentState().addRecordWithDegree(stateEntry.getKey(), toIntExact(stateEntry.getValue()));
                }
            }
        }

        //System.out.println("Current VertexDepr Partitioning Table: " + vertexPartition);

        if (TEMPGLOBALVARIABLES.printPhaseTwo) {
            System.out.println(countBroadcastsOnWorker + ": " + vertexDegreeMap);
        }

        if (waitingEdges.size() > 0) {
            List<Edge<Long, NullValue>> toBeRemoved = new ArrayList<>();
            for (Edge<Long, NullValue> e : waitingEdges) {
                if (checkIfEarlyArrived(e)) {
                    //System.out.println("checking again");
                    int partitionId = modelBuilder.choosePartition(e);
                    out.collect(new Tuple2<>(e, partitionId));
                    //System.out.println("added EdgeDepr (" + e.getEdge().f0 + " " + e.getEdge().f1 + ") later");
                    toBeRemoved.add(e);
                } else {
                    //System.out.println("breaking");
                    break;

                }
            }
            waitingEdges.removeAll(toBeRemoved);
        }
        //ctx.output(GraphPartitionerImpl.outputTag, "1: " + modelBuilder.getHdrf().getCurrentState().printState().toString());

    }

    @Override
    public void processElement(Edge<Long, NullValue> currentEdge, ReadOnlyContext ctx, Collector<Tuple2<Edge<Long, NullValue>,Integer>> out) throws Exception {

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
        //int partitionId = modelBuilder.choosePartition(currentEdge);
        //System.out.println("Phase 2: " + currentEdge.getEdge().getOriginVertexDepr() + " " + currentEdge.getEdge().getDestinVertexDepr() + " --> " + partitionId);

        // Add to "SINK" (TODO: Real Sink Function)
        //edgeInPartition.put(currentEdge,partitionId);

        //if (PhasePartitionerGelly.printPhaseTwo == true) {
            //System.out.println(checkInside);

        //out.collect(new Tuple2<>(currentEdge,partitionId));

        //ctx.output(GraphPartitionerImpl.outputTag, "1: " + modelBuilder.getHdrf().getCurrentState().printState().toString());

    }

    private boolean checkIfEarlyArrived(Edge<Long, NullValue> currentEdge) {

        //System.out.println(vertexDegreeMap.containsKey(currentEdge.getEdge().f0) + " " + currentEdge.getEdge().f0 + " __ " + currentEdge.getEdge().f1 + vertexDegreeMap.containsKey(currentEdge.getEdge().f1));
        boolean sourceInside = false;
        boolean targetInside = false;
        if (algorithm.equals("hdrf")) {
            sourceInside = modelBuilder.getHdrf().getCurrentState().checkIfRecordExits(Long.parseLong(currentEdge.f0.toString()));
            targetInside = modelBuilder.getHdrf().getCurrentState().checkIfRecordExits(Long.parseLong(currentEdge.f1.toString()));
        } else if (algorithm.equals("dbh")) {
            sourceInside = modelBuilder.getDbh().getCurrentState().checkIfRecordExits(Long.parseLong(currentEdge.f0.toString()));
            targetInside = modelBuilder.getDbh().getCurrentState().checkIfRecordExits(Long.parseLong(currentEdge.f1.toString()));
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

}



