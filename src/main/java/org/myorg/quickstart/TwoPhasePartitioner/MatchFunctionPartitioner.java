package org.myorg.quickstart.TwoPhasePartitioner;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.partitioners.PhasePartitionerHdrf;
import org.myorg.quickstart.sharedState.EdgeEvent;
import org.myorg.quickstart.sharedState.PhasePartitioner;

import java.util.*;

import static java.lang.Math.toIntExact;

public class MatchFunctionPartitioner extends KeyedBroadcastProcessFunction<Integer, EdgeEventGelly, HashMap<Long, Long>, Tuple2<EdgeEventGelly,Integer>> {

    int countBroadcastsOnWorker = 0;
    int counterEdgesInstance = 0;
    String algorithm;
    HashMap<Long, Long> vertexDegreeMap = new HashMap<>();
    ModelBuilderGelly modelBuilder;
    List<EdgeEventGelly> waitingEdges;

    public MatchFunctionPartitioner(String algorithm, Integer k, double lambda) {
        this.algorithm = algorithm;
        this.waitingEdges = new ArrayList<>();
        if (algorithm.equals("hdrf"))
            this.modelBuilder = new ModelBuilderGelly(algorithm, vertexDegreeMap, k, lambda);
    }


    // This function is called every time when a broadcast state is processed from the previous phase
    @Override
    public void processBroadcastElement(HashMap<Long, Long> broadcastElement, Context ctx, Collector<Tuple2<EdgeEventGelly,Integer>> out) throws Exception {

        countBroadcastsOnWorker++;

        // Print for debugging
        if (PhasePartitionerHdrf.printPhaseOne)
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

        //System.out.println("Current Vertex Partitioning Table: " + vertexPartition);

        if (PhasePartitionerHdrf.printPhaseTwo) {
            System.out.println(countBroadcastsOnWorker + ": " + vertexDegreeMap);
        }

        if (waitingEdges.size() > 0) {
            List<EdgeEventGelly> toBeRemoved = new ArrayList<>();
            for (EdgeEventGelly e : waitingEdges) {
                if (checkIfEarlyArrived(e)) {
                    //System.out.println("checking again");
                    int partitionId = modelBuilder.choosePartition(e);
                    out.collect(new Tuple2<>(e, partitionId));
                    //System.out.println("added Edge (" + e.getEdge().f0 + " " + e.getEdge().f1 + ") later");
                    toBeRemoved.add(e);
                } else {
                    //System.out.println("breaking");
                    break;

                }
            }
            waitingEdges.removeAll(toBeRemoved);
        }
        ctx.output(PhasePartitionerHdrf.outputTag, "1: " + modelBuilder.getHdrf().getCurrentState().printState().toString());

    }

    @Override
    public void processElement(EdgeEventGelly currentEdge, ReadOnlyContext ctx, Collector<Tuple2<EdgeEventGelly,Integer>> out) throws Exception {

        //System.out.println("inside Process: Edge (" + currentEdge.getEdge().f0 + " " + currentEdge.getEdge().f1 + "): " + currentEdge.getEdge().f0.getClass() + " " + currentEdge.getEdge().f1.getClass() + " -- ");
        boolean checkInside = checkIfEarlyArrived(currentEdge);

        if (!checkInside) {
            //System.out.println("Edge (" + currentEdge.getEdge().f0 + " " + currentEdge.getEdge().f1 + ") not inside. added to queue");
            waitingEdges.add(currentEdge);
        } else {
            int partitionId = modelBuilder.choosePartition(currentEdge);
            out.collect(new Tuple2<>(currentEdge, partitionId));
        }

        counterEdgesInstance++;
        //int partitionId = modelBuilder.choosePartition(currentEdge);
        //System.out.println("Phase 2: " + currentEdge.getEdge().getOriginVertex() + " " + currentEdge.getEdge().getDestinVertex() + " --> " + partitionId);

        // Add to "SINK" (TODO: Real Sink Function)
        //edgeInPartition.put(currentEdge,partitionId);

        //if (PhasePartitionerGelly.printPhaseTwo == true) {
            //System.out.println(checkInside);

        //out.collect(new Tuple2<>(currentEdge,partitionId));

        ctx.output(PhasePartitionerHdrf.outputTag, "1: " + modelBuilder.getHdrf().getCurrentState().printState().toString());

    }

    private boolean checkIfEarlyArrived(EdgeEventGelly currentEdge) {

        //System.out.println(vertexDegreeMap.containsKey(currentEdge.getEdge().f0) + " " + currentEdge.getEdge().f0 + " __ " + currentEdge.getEdge().f1 + vertexDegreeMap.containsKey(currentEdge.getEdge().f1));
        boolean sourceInside = modelBuilder.getHdrf().getCurrentState().checkIfRecordExits(Long.parseLong(currentEdge.getEdge().f0.toString()));
        boolean targetInside = modelBuilder.getHdrf().getCurrentState().checkIfRecordExits(Long.parseLong(currentEdge.getEdge().f1.toString()));
        //boolean sourceInside = vertexDegreeMap.keySet().contains(currentEdge.getEdge().f0);
        //boolean targetInside = vertexDegreeMap.keySet().contains(currentEdge.getEdge().f1);

        // Debugging only
        if (PhasePartitionerHdrf.printPhaseTwo) {
            List<Tuple3> printState = modelBuilder.getHdrf().getCurrentState().printState();
            System.out.println("Edge (" + currentEdge.getEdge().f0 + " " + currentEdge.getEdge().f1 + "): " + printState);
        }

        // Return TRUE if both vertices are in HashMap. Otherwise return false
        if (sourceInside && targetInside) {
            return true;
        } else {
            return false;
        }

    }

}



