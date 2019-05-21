package org.myorg.quickstart.deprecated.old;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.myorg.quickstart.deprecated.VertexDepr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.myorg.quickstart.deprecated.old.PartitionWithBroadcast.tupleTypeInfo;

public class MatchFunction extends KeyedBroadcastProcessFunction<VertexDepr, EdgeDepr, Tuple2<VertexDepr, List<Integer>>, String> {

    private int counter = 0;
    private ArrayList<Tuple2<VertexDepr, List<Integer>>> knownState = new ArrayList<>();

    private final MapStateDescriptor<String, Tuple2<VertexDepr, List<Integer>>> broadcastStateDescriptor =
            new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, tupleTypeInfo);

    @Override
    public void processBroadcastElement(Tuple2<VertexDepr, List<Integer>> broadcastElement, Context ctx, Collector<String> out) throws Exception {

        System.out.println("ctx entries before processing " + broadcastElement.f0.getId() + " (in: " + broadcastElement.f1.get(0) + "," + broadcastElement.f1.get(1) + ") --> " + ctx.getBroadcastState(broadcastStateDescriptor).entries());

        boolean inList = false;
        ctx.getBroadcastState(broadcastStateDescriptor).put("Entry_" + counter++, broadcastElement);
        for (Map.Entry<String, Tuple2<VertexDepr, List<Integer>>> stateEntry : ctx.getBroadcastState(broadcastStateDescriptor).entries()) {
            if (stateEntry.getValue().f0.getId() == broadcastElement.f0.getId())
                System.out.println("found existing -- id from entry: " + stateEntry.getValue().f0.getId() + " -- id from broadcast " + broadcastElement.f0.getId());
                inList = true;
        }
        if (inList == false) {
            ctx.getBroadcastState(broadcastStateDescriptor).put("Entry_" + counter++, broadcastElement);
            System.out.println("Add RULE " + broadcastElement.f0.getId() + " to state table");
        }
        System.out.println("ctx entries after processing " + broadcastElement.f0.getId() + " --> " + ctx.getBroadcastState(broadcastStateDescriptor).entries());
    }

    @Override
    public void processElement(EdgeDepr currentEdgeDepr, ReadOnlyContext ctx, Collector<String> out) throws Exception {

        final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

        System.out.println("EDGE: " + currentEdgeDepr.getOriginVertexDepr().getId() + " " + currentEdgeDepr.getDestinVertexDepr().getId());

        // Iterate through all "vertexTable" rows
        for (Map.Entry<String, Tuple2<VertexDepr, List<Integer>>> entry: ctx.getBroadcastState(broadcastStateDescriptor).immutableEntries()) {

            // Get all "partitions" that this vertex (row) is partitioned to ## yes, ugly way of doing it
            String currentPartitions = ""; for (Integer i: entry.getValue().f1) { currentPartitions = currentPartitions + "," + i; }

            for (VertexDepr v: currentEdgeDepr.getVertices()) {
                if (entry.getValue().f0.getId() == v.getId()) {
                    System.out.println("OLD VertexDepr " + v.getId() + " already in vertexTable (partitions: " + currentPartitions + ")");
                } else {
                    StringBuilder sb = new StringBuilder();
                    sb.append(v.getId());
                    //out.collect("New VertexDepr " + sb.toString() + " to be added to state table. " + "( currentPart: " + currentPartitions);
                    out.collect("New VertexDepr " + sb.toString() + " to be added to state table. for current entry: " + entry.getValue().f0.getId() + " with part: " + currentPartitions);
                    ctx.output(outputTag, "sideout-" + String.valueOf(sb.toString()));

                }
            }
        }
    }

}

