package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.myorg.quickstart.sharedState.PartitionWithBroadcast.tupleTypeInfo;

public class MatchFunctionRule extends KeyedBroadcastProcessFunction<Integer, EdgeSimple, Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>> {

    private int counter = 0;
    private ArrayList<Tuple2<Integer, List<Integer>>> knownState = new ArrayList<>();

    private final MapStateDescriptor<String, Tuple2<Integer, List<Integer>>> broadcastStateDescriptor =
            new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, tupleTypeInfo);

    @Override
    public void processBroadcastElement(Tuple2<Integer, List<Integer>> broadcastElement, Context ctx, Collector<Tuple2<Integer, List<Integer>>> out) throws Exception {

            System.out.println("ctx entries before processing " + broadcastElement.f0 + " (in: " + broadcastElement.f1.get(0) + ") --> " + ctx.getBroadcastState(broadcastStateDescriptor).entries());
            //System.out.println("ctx entries before processing " + broadcastElement.f0 + " (in: " + broadcastElement.f1.get(0) + "," + broadcastElement.f1.get(1) + ") --> " + ctx.getBroadcastState(broadcastStateDescriptor).entries());

            boolean inList = false;
            ctx.getBroadcastState(broadcastStateDescriptor).put("Entry_" + counter++, broadcastElement);
            for (Map.Entry<String, Tuple2<Integer, List<Integer>>> stateEntry : ctx.getBroadcastState(broadcastStateDescriptor).entries()) {
                if (stateEntry.getValue().f0 == broadcastElement.f0)
                    System.out.println("found existing -- id from entry: " + stateEntry.getValue().f0 + " -- id from broadcast " + broadcastElement.f0);
                inList = true;
            }
            if (inList == false) {
                ctx.getBroadcastState(broadcastStateDescriptor).put("Entry_" + counter++, broadcastElement);
                System.out.println("Add RULE " + broadcastElement.f0 + " to state table");
            }
            System.out.println("ctx entries after processing " + broadcastElement.f0 + " --> " + ctx.getBroadcastState(broadcastStateDescriptor).entries());

    }

    @Override
    public void processElement(EdgeSimple currentEdge, ReadOnlyContext ctx, Collector<Tuple2<Integer, List<Integer>>> out) throws Exception {

        System.out.println("EDGE: " + currentEdge.getOriginVertex() + " " + currentEdge.getDestinVertex());
        List<Integer> currentPartitions = new ArrayList<>();

        // Iterate through all "stateTable" rows
        for (Map.Entry<String, Tuple2<Integer, List<Integer>>> entry: ctx.getBroadcastState(broadcastStateDescriptor).immutableEntries()) {

            // Get all "partitions" that this vertex (row) is partitioned to
            for (Integer i: entry.getValue().f1) { currentPartitions.add(i); }

            for (Integer v: currentEdge.getVertices()) {
                if (entry.getValue().f0 == v) {
                    System.out.println("OLD Vertex " + v + " already in stateTable (partitions: " + currentPartitions + ")");
                } else {
                    List<Integer> partitions = new ArrayList<>();
                    int partitionId = choosePartition(v);
                    partitions.add(partitionId);
                    Tuple2<Integer, List<Integer>> tuple = new Tuple2(v,partitions);
                    out.collect(tuple);
                    System.out.println("New vertex found: ID = " + v + " -- Placed in Partition " + partitionId);

                }
            }
        }
    }

    public int choosePartition(int vertexId) {
        Random rand = new Random();
        return rand.nextInt(1);

    }

}

