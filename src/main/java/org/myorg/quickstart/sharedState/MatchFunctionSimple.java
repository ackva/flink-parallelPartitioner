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

import static org.myorg.quickstart.sharedState.PartitionWithBroadcast.tupleTypeInfo;

public class MatchFunctionSimple extends KeyedBroadcastProcessFunction<Integer, EdgeSimple, Tuple2<Integer, List<Integer>>, String> {

    private int counter = 0;
    private ArrayList<Tuple2<Integer, List<Integer>>> knownState = new ArrayList<>();

    private final MapStateDescriptor<String, Tuple2<Integer, List<Integer>>> broadcastStateDescriptor =
            new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, tupleTypeInfo);

    @Override
    public void processBroadcastElement(Tuple2<Integer, List<Integer>> broadcastElement, Context ctx, Collector<String> out) throws Exception {

        System.out.println("ctx entries before processing " + broadcastElement.f0 + " (in: " + broadcastElement.f1.get(0) + "," + broadcastElement.f1.get(1) + ") --> " + ctx.getBroadcastState(broadcastStateDescriptor).entries());

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
    public void processElement(EdgeSimple currentEdge, ReadOnlyContext ctx, Collector<String> out) throws Exception {

        final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

        System.out.println("EDGE: " + currentEdge.getOriginVertex() + " " + currentEdge.getDestinVertex());

        // Iterate through all "stateTable" rows
        for (Map.Entry<String, Tuple2<Integer, List<Integer>>> entry: ctx.getBroadcastState(broadcastStateDescriptor).immutableEntries()) {

            // Get all "partitions" that this vertex (row) is partitioned to ## yes, ugly way of doing it
            String currentPartitions = ""; for (Integer i: entry.getValue().f1) { currentPartitions = currentPartitions + "," + i; }

            for (Integer v: currentEdge.getVertices()) {
                if (entry.getValue().f0 == v) {
                    System.out.println("OLD Vertex " + v + " already in stateTable (partitions: " + currentPartitions + ")");
                } else {
                    StringBuilder sb = new StringBuilder();
                    sb.append(v);
                    //out.collect("New Vertex " + sb.toString() + " to be added to state table. " + "( currentPart: " + currentPartitions);
                    out.collect("New Vertex " + sb.toString() + " to be added to state table. for current entry: " + entry.getValue().f0 + " with part: " + currentPartitions);
                    ctx.output(outputTag, "sideout-" + String.valueOf(sb.toString()));

                }
            }
        }
    }

}

