package org.myorg.quickstart.ForGelly;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.sharedState.PhasePartitioner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import static java.time.Instant.now;

//import static org.myorg.quickstart.sharedState.old.PartitionWithBroadcast.tupleTypeInfo;

public class MatchFunctionEdgesGelly extends KeyedBroadcastProcessFunction<Integer, EdgeEventGelly, HashMap, Tuple2<EdgeEventGelly,Integer>> {

    int countBroadcastsOnWorker = 0;
    int counterEdgesInstance = 0;
    HashMap<Integer, HashSet<Integer>> vertexPartition = new HashMap<>();
    HashMap<EdgeEventGelly, Integer> edgeInPartition = new HashMap<>();
    ModelBuilderGelly modelBuilder;

    public MatchFunctionEdgesGelly(String algorithm) {
        this.modelBuilder = new ModelBuilderGelly(algorithm, vertexPartition);
    }


    // This function is called every time when a broadcast state is processed from the previous phase
    @Override
    public void processBroadcastElement(HashMap broadcastElement, Context ctx, Collector<Tuple2<EdgeEventGelly,Integer>> out) throws Exception {

        countBroadcastsOnWorker++;
        //System.out.println("Phase 2: Broadcasting HashMap " + broadcastElement);

        // ### Merge local model from Phase 1 with global model, here in Phase 2
        Iterator it = broadcastElement.entrySet().iterator();
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

        //System.out.println("Current Vertex Partitioning Table: " + vertexPartition);

        if (PhasePartitionerGelly.printPhaseTwo == true) {
            //System.out.println(printString);
        }

        //ctx.output(PhasePartitioner.outputTag, "Phase 2: Broadcasting HashMap " + broadcastElement + " " + now() + " - " + countBroadcastsOnWorker);
        //System.out.println("BROAD: " + counterEdges + " Edges processed -- " + ++counterBroadcast + " state entries");

    }

    @Override
    public void processElement(EdgeEventGelly currentEdge, ReadOnlyContext ctx, Collector<Tuple2<EdgeEventGelly,Integer>> out) throws Exception {

        String checkInside = checkIfEarlyArrival(currentEdge);

        counterEdgesInstance++;
        int partitionId = modelBuilder.choosePartition(currentEdge);
        //System.out.println("Phase 2: " + currentEdge.getEdge().getOriginVertex() + " " + currentEdge.getEdge().getDestinVertex() + " --> " + partitionId);

        // Add to "SINK" (TODO: Real Sink Function)
        edgeInPartition.put(currentEdge,partitionId);


        out.collect(new Tuple2<>(currentEdge,partitionId));

        ctx.output(PhasePartitioner.outputTag, checkInside);
        //System.out.println("EDGES: " + ++counterEdges + " Edges processed -- " + counterBroadcast + " Broadcast entries");

    }

    private String checkIfEarlyArrival(EdgeEventGelly currentEdge) {
        String returnString = "Edge (" + currentEdge.getEdge().f0 + " " + currentEdge.getEdge().f1 + "): ";
        String vertexPartitionString = vertexPartition.toString();
        if (!vertexPartition.containsKey(currentEdge.getEdge().f0)) {
            returnString = returnString + "Vertex: " + currentEdge.getEdge().f0 + " outside -- ";
        }
        else {
            returnString = returnString + "Vertex: " + currentEdge.getEdge().f0 + " inside  -- ";
        }
        if (!vertexPartition.containsKey(currentEdge.getEdge().f1)){
            returnString = returnString + "Vertex: " + currentEdge.getEdge().f1 + " outside -- " + vertexPartitionString;
        }
        else {
            returnString = returnString + "Vertex: " + currentEdge.getEdge().f1 + " inside  -- " + vertexPartitionString;
        }

        return returnString;
    }

}

