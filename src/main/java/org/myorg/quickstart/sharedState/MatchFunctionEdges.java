package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;

import static java.time.Instant.now;

//import static org.myorg.quickstart.sharedState.old.PartitionWithBroadcast.tupleTypeInfo;

public class MatchFunctionEdges extends KeyedBroadcastProcessFunction<Integer, EdgeEvent, HashMap, Tuple2<EdgeEvent,Integer>> {

    int counterBroadcastInstance = 0;
    int counterEdgesInstance = 0;
    HashMap<Integer, HashSet<Integer>> vertexPartition = new HashMap<>();
    HashMap<EdgeEvent, Integer> edgeInPartition = new HashMap<>();
    ModelBuilder modelBuilder = new ModelBuilder("byOrigin", vertexPartition);

    @Override
    public void processBroadcastElement(HashMap broadcastElement, Context ctx, Collector<Tuple2<EdgeEvent,Integer>> out) throws Exception {

        counterBroadcastInstance++;
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


/*        String printString = " - ";
        printString = printString + e.getEdge().getOriginVertex() + " " + e.getEdge().getDestinVertex() + ", ";

        if (PhasePartitioner.printPhaseOne == true) {
            printString = now() + "P1: window # " + windowCounter + " -- edges: " + edgesInWindow.size() + printString + " --(Edges)";
            System.out.println(printString);
        }*/

        ctx.output(PhasePartitioner.outputTag, "Phase 2: Broadcasting HashMap " + broadcastElement + " " + now() + " - " + counterBroadcastInstance);
        //System.out.println("BROAD: " + counterEdges + " Edges processed -- " + ++counterBroadcast + " state entries");

    }

    @Override
    public void processElement(EdgeEvent currentEdge, ReadOnlyContext ctx, Collector<Tuple2<EdgeEvent,Integer>> out) throws Exception {

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

    private String checkIfEarlyArrival(EdgeEvent currentEdge) {
        String returnString = "Edge (" + currentEdge.getEdge().getOriginVertex() + " " + currentEdge.getEdge().getDestinVertex() + "): ";
        String vertexPartitionString = vertexPartition.toString();
        if (!vertexPartition.containsKey(currentEdge.getEdge().getOriginVertex())) {
            returnString = returnString + "Vertex: " + currentEdge.getEdge().getOriginVertex() + " outside -- ";
        }
        else {
            returnString = returnString + "Vertex: " + currentEdge.getEdge().getOriginVertex() + " inside  -- ";
        }
        if (!vertexPartition.containsKey(currentEdge.getEdge().getDestinVertex())){
            returnString = returnString + "Vertex: " + currentEdge.getEdge().getDestinVertex() + " outside -- " + vertexPartitionString;
        }
        else {
            returnString = returnString + "Vertex: " + currentEdge.getEdge().getDestinVertex() + " inside  -- " + vertexPartitionString;
        }

        return returnString;
    }

}

