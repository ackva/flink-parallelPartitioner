package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

import static org.myorg.quickstart.sharedState.PartitionWithBroadcast.tupleTypeInfo;

public class MatchFunctionEdges extends KeyedBroadcastProcessFunction<Integer, EdgeEvent, HashMap, Tuple2<EdgeEvent,Integer>> {

    int counterBroadcast = 0;
    int counterEdges = 0;
    HashMap<Integer, HashSet<Integer>> vertexPartition = new HashMap<>();
    HashMap<EdgeEvent, Integer> edgeInPartition = new HashMap<>();
    ModelBuilder modelBuilder = new ModelBuilder("byOrigin", vertexPartition);

    private final MapStateDescriptor<String, Tuple2<Integer, HashSet<Integer>>> broadcastStateDescriptor =
            new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, tupleTypeInfo);

    @Override
    public void processBroadcastElement(HashMap broadcastElement, Context ctx, Collector<Tuple2<EdgeEvent,Integer>> out) throws Exception {

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

        System.out.println("BROAD: " + counterEdges + " Edges processed -- " + ++counterBroadcast + " state entries");

    }

    @Override
    public void processElement(EdgeEvent currentEdge, ReadOnlyContext ctx, Collector<Tuple2<EdgeEvent,Integer>> out) throws Exception {

        int partitionId = modelBuilder.choosePartition(currentEdge);
        //System.out.println("Phase 2: " + currentEdge.getEdge().getOriginVertex() + " " + currentEdge.getEdge().getDestinVertex() + " --> " + partitionId);

        // Add to "SINK" (TODO: Real Sink Function)
        edgeInPartition.put(currentEdge,partitionId);

        out.collect(new Tuple2<>(currentEdge,partitionId));

        System.out.println("EDGES: " + ++counterEdges + " Edges processed -- " + counterBroadcast + " Broadcast entries");

    }

}

