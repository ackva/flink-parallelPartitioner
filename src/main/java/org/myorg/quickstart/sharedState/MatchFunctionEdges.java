package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

import static org.myorg.quickstart.sharedState.PartitionWithBroadcast.tupleTypeInfo;

public class MatchFunctionEdges extends KeyedBroadcastProcessFunction<Integer, EdgeEvent, HashMap, Tuple2<EdgeEvent,Integer>> {

    int counter = 0;
    HashMap<Integer, HashSet<Integer>> vertexPartition = new HashMap<>();
    HashMap<EdgeEvent, Integer> edgeInPartition = new HashMap<>();
    private String processedEdges = "Edges processed by: ";
    private String processedBroadcastElements = "BroadcastRules processed by: ";
    private int round;
    ModelBuilder modelBuilder = new ModelBuilder("byOrigin", vertexPartition);


    public void setRound(int round) {
        this.round = round;
    }

    private final MapStateDescriptor<String, Tuple2<Integer, HashSet<Integer>>> broadcastStateDescriptor =
            new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, tupleTypeInfo);

    @Override
    //public void processBroadcastElement(Tuple2<Integer, List<Integer>> broadcastElement, Context ctx, Collector<Integer> out) throws Exception {
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

        /*
        // NOT SURE IF NEEDED
        Integer[] vertices = new Integer[2];
        vertices[0] = currentEdge.getEdge().getOriginVertex();
        vertices[1] = currentEdge.getEdge().getDestinVertex();

        for (int i = 0; i < 2; i++) {
            HashSet<Integer> partitionSet = new HashSet<>();
            if (vertexPartition.containsKey(vertices[i])) {
                String partitionsString = vertices[i] + " in: ";
                for(Integer partition: vertexPartition.get(vertices[0])) {
                    partitionSet.add(partition);
                    partitionsString = partitionsString + ", ";
                }
                partitionSet.add(partitionId);
                vertexPartition.put(vertices[i], partitionSet);
            } else {
                partitionSet.add(partitionId);
                vertexPartition.put(vertices[i], partitionSet);
            }
        }
        */

        //System.out.println("Updated Map after broadcast of: " + broadcastElement + " --- " + vertexPartition);

    }

    @Override
    public void processElement(EdgeEvent currentEdge, ReadOnlyContext ctx, Collector<Tuple2<EdgeEvent,Integer>> out) throws Exception {

        //System.out.println("Phase 2: Processing EDGE: " + currentEdge.getEdge().getOriginVertex() + " " + currentEdge.getEdge().getDestinVertex());

/*        Integer[] vertices = new Integer[2];
        vertices[0] = currentEdge.getEdge().getOriginVertex();
        vertices[1] = currentEdge.getEdge().getDestinVertex();*/

        int partitionId = modelBuilder.choosePartition(currentEdge);


/*        // Get Partition (TODO: real partitioning Algorithm please)
        Random rand = new Random();
        int partitionId = rand.nextInt(4);*/

        // Add to "SINK" (TODO: Real Sink Function)
        edgeInPartition.put(currentEdge,partitionId);


        /*for (int i = 0; i < 2; i++) {
            HashSet<Integer> partitionSet = new HashSet<>();
            if (vertexPartition.containsKey(vertices[i])) {
                String partitionsString = vertices[i] + " in: ";
                for(Integer partition: vertexPartition.get(vertices[0])) {
                    partitionSet.add(partition);
                    partitionsString = partitionsString + ", ";
                }
                partitionSet.add(partitionId);
                vertexPartition.put(vertices[i], partitionSet);
            } else {
                partitionSet.add(partitionId);
                vertexPartition.put(vertices[i], partitionSet);
            }

        }*/

        out.collect(new Tuple2<>(currentEdge,partitionId));
        }

}

