package org.myorg.quickstart.deprecated;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

import static java.time.Instant.now;

//import static org.myorg.quickstart.deprecated.old.PartitionWithBroadcast.tupleTypeInfo;

public class MatchFunctionEdges extends KeyedBroadcastProcessFunction<Integer, EdgeEventDepr, HashMap, Tuple2<EdgeEventDepr,Integer>> {

    int counterBroadcastInstance = 0;
    int counterEdgesInstance = 0;
    HashMap<Integer, HashSet<Integer>> vertexPartition = new HashMap<>();
    HashMap<EdgeEventDepr, Integer> edgeInPartition = new HashMap<>();
    ModelBuilderDepr modelBuilderDepr = new ModelBuilderDepr("byOrigin", vertexPartition);

    @Override
    public void processBroadcastElement(HashMap broadcastElement, Context ctx, Collector<Tuple2<EdgeEventDepr,Integer>> out) throws Exception {

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
        printString = printString + e.getEdge().getOriginVertexDepr() + " " + e.getEdge().getDestinVertexDepr() + ", ";

        if (TEMPGLOBALVARIABLES.printPhaseOne == true) {
            printString = now() + "P1: window # " + windowCounter + " -- edges: " + edgesInWindow.size() + printString + " --(Edges)";
            System.out.println(printString);
        }*/

        ctx.output(PhasePartitioner.outputTag, "Phase 2: Broadcasting HashMap " + broadcastElement + " " + now() + " - " + counterBroadcastInstance);
        //System.out.println("BROAD: " + counterEdges + " Edges processed -- " + ++counterBroadcast + " state entries");

    }

    @Override
    public void processElement(EdgeEventDepr currentEdge, ReadOnlyContext ctx, Collector<Tuple2<EdgeEventDepr,Integer>> out) throws Exception {

        String checkInside = checkIfEarlyArrival(currentEdge);

        counterEdgesInstance++;
        int partitionId = modelBuilderDepr.choosePartition(currentEdge);
        //System.out.println("Phase 2: " + currentEdge.getEdge().getOriginVertexDepr() + " " + currentEdge.getEdge().getDestinVertexDepr() + " --> " + partitionId);

        // Add to "SINK" (TODO: Real Sink Function)
        edgeInPartition.put(currentEdge,partitionId);


        out.collect(new Tuple2<>(currentEdge,partitionId));

        ctx.output(PhasePartitioner.outputTag, checkInside);
        //System.out.println("EDGES: " + ++counterEdges + " Edges processed -- " + counterBroadcast + " Broadcast entries");

    }

    private String checkIfEarlyArrival(EdgeEventDepr currentEdge) {
        String returnString = "EdgeDepr (" + currentEdge.getEdge().getOriginVertex() + " " + currentEdge.getEdge().getDestinVertex() + "): ";
        String vertexPartitionString = vertexPartition.toString();
        if (!vertexPartition.containsKey(currentEdge.getEdge().getOriginVertex())) {
            returnString = returnString + "VertexDepr: " + currentEdge.getEdge().getOriginVertex() + " outside -- ";
        }
        else {
            returnString = returnString + "VertexDepr: " + currentEdge.getEdge().getOriginVertex() + " inside  -- ";
        }
        if (!vertexPartition.containsKey(currentEdge.getEdge().getDestinVertex())){
            returnString = returnString + "VertexDepr: " + currentEdge.getEdge().getDestinVertex() + " outside -- " + vertexPartitionString;
        }
        else {
            returnString = returnString + "VertexDepr: " + currentEdge.getEdge().getDestinVertex() + " inside  -- " + vertexPartitionString;
        }

        return returnString;
    }

}

