/*

package org.myorg.quickstart.deprecated.TwoPhasePartitioner;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.deprecated.PhasePartitioner;
import org.myorg.quickstart.utils.EdgeEventGelly;
import org.myorg.quickstart.utils.ModelBuilderGelly;
import org.myorg.quickstart.utils.TEMPGLOBALVARIABLES;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import static java.time.Instant.now;

//import static org.myorg.quickstart.deprecated.old.PartitionWithBroadcast.tupleTypeInfo;

public class MatchFunctionEdgesGelly extends KeyedBroadcastProcessFunction<Integer, EdgeEventGelly, HashMap, Tuple2<EdgeEventGelly,Integer>> {

    int countBroadcastsOnWorker = 0;
    int counterEdgesInstance = 0;
    HashMap<Integer, HashSet<Integer>> vertexPartition = new HashMap<>();
    HashMap<EdgeEventGelly, Integer> edgeInPartition = new HashMap<>();
    ModelBuilderGelly modelBuilder;

    public MatchFunctionEdgesGelly(String algorithm) {
        //this.modelBuilder = new ModelBuilderGelly(algorithm, vertexPartition);
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

        //System.out.println("Current VertexDepr Partitioning Table: " + vertexPartition);

        if (TEMPGLOBALVARIABLES.printPhaseTwo == true) {
            //System.out.println(printString);
        }

        //ctx.output(PhasePartitioner.outputTag, "Phase 2: Broadcasting HashMap " + broadcastElement + " " + now() + " - " + countBroadcastsOnWorker);
        //System.out.println("BROAD: " + counterEdges + " Edges processed -- " + ++counterBroadcast + " state entries");

    }

    @Override
    public void processElement(Edge<Long, NullValue> currentEdge, ReadOnlyContext ctx, Collector<Tuple2<EdgeEventGelly,Integer>> out) throws Exception {

        String checkInside = checkIfEarlyArrival(currentEdge);

        counterEdgesInstance++;
        int partitionId = modelBuilder.choosePartition(currentEdge);
        //System.out.println("Phase 2: " + currentEdge.getEdge().getOriginVertexDepr() + " " + currentEdge.getEdge().getDestinVertexDepr() + " --> " + partitionId);

        // Add to "SINK" (TODO: Real Sink Function)
        edgeInPartition.put(currentEdge,partitionId);


        out.collect(new Tuple2<>(currentEdge,partitionId));

        ctx.output(PhasePartitioner.outputTag, checkInside);
        //System.out.println("EDGES: " + ++counterEdges + " Edges processed -- " + counterBroadcast + " Broadcast entries");

    }

    private String checkIfEarlyArrival(EdgeEventGelly currentEdge) {
        String returnString = "EdgeDepr (" + currentEdge.getEdge().f0 + " " + currentEdge.getEdge().f1 + "): ";
        String vertexPartitionString = vertexPartition.toString();
        if (!vertexPartition.containsKey(currentEdge.getEdge().f0)) {
            returnString = returnString + "VertexDepr: " + currentEdge.getEdge().f0 + " outside -- ";
        }
        else {
            returnString = returnString + "VertexDepr: " + currentEdge.getEdge().f0 + " inside  -- ";
        }
        if (!vertexPartition.containsKey(currentEdge.getEdge().f1)){
            returnString = returnString + "VertexDepr: " + currentEdge.getEdge().f1 + " outside -- " + vertexPartitionString;
        }
        else {
            returnString = returnString + "VertexDepr: " + currentEdge.getEdge().f1 + " inside  -- " + vertexPartitionString;
        }

        return returnString;
    }

}


*/
