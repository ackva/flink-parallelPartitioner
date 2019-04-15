/*
package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.myorg.quickstart.sharedState.PartitionWithBroadcast.tupleTypeInfo;




//public class MatchFunctionRule extends KeyedBroadcastProcessFunction<Integer, EdgeSimple, Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>> {

public class MergeCurrentState {

    private int counter = 0;

*/
/*
    private final MapStateDescriptor<String, Tuple2<Integer, List<Integer>>> broadcastStateDescriptor =
            new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, tupleTypeInfo);
*//*


    @Override
    public void processBroadcastElement(Tuple2<Integer, List<Integer>> broadcastElement, Context ctx, Collector<Tuple2<Integer, List<Integer>>> out) throws Exception {
        //Instant instant = Instant.now();
        System.out.println("Broadcasting " + broadcastElement.f0 + " now");
        //System.out.println("Entries: " + counter + " --> " + ctx.getBroadcastState(broadcastStateDescriptor).entries());

        if (broadcastElement.f0 != -1) {
            boolean inList = false;
            //ctx.getBroadcastState(broadcastStateDescriptor).put("Entry_" + counter++, broadcastElement);
            for (Map.Entry<String, Tuple2<Integer, List<Integer>>> stateEntry : ctx.getBroadcastState(broadcastStateDescriptor).entries()) {
                if (stateEntry.getValue().f0 == broadcastElement.f0) {
                    //System.out.println("Current State entry " + stateEntry.getValue().f0 + " --- Current BroadcastElement entry: " + broadcastElement.f0);
                    //System.out.println("found existing -- id from entry: " + stateEntry.getValue().f0 + " -- id from broadcast " + broadcastElement.f0);
                    inList = true;
                }
            }
            //System.out.println("In List is "+ inList + " for broadcastelement " + broadcastElement.f0);
            if (inList == false) {
                //ctx.getBroadcastState(broadcastStateDescriptor).put("Entry_" + counter++, broadcastElement);
                ctx.getBroadcastState(broadcastStateDescriptor).put(broadcastElement.f0.toString(), broadcastElement);
                System.out.println("Add RULE: Vertex " + broadcastElement.f0 + " to state table");
                counter++;
                System.out.println("State Table after processing " + broadcastElement.f0 + " --> " + ctx.getBroadcastState(broadcastStateDescriptor).entries());
            }
        }

    }

    @Override
    public void processElement(EdgeSimple currentEdge, ReadOnlyContext ctx, Collector<Tuple2<Integer, List<Integer>>> out) throws Exception {

        List<Integer> currentPartitions = new ArrayList<>();
        boolean addToList;
        // Iterate through both vertices of the current edge and compare it with the existing entries from the state table
        for (Integer currentVertex: currentEdge.getVertices()) {
            addToList = true;
            stateLoop:
            for (Map.Entry<String, Tuple2<Integer, List<Integer>>> stateEntry: ctx.getBroadcastState(broadcastStateDescriptor).immutableEntries()) {
                if (stateEntry.getValue().f0 == currentVertex) {
                    System.out.println("current vertex: " + currentVertex);
                    addToList = false;
                    break stateLoop;
                }
            }
            if (addToList == true) {
                List<Integer> partitions = new ArrayList<>();
                partitions.add(choosePartition(currentVertex));
                Tuple2<Integer, List<Integer>> tuple = new Tuple2(currentVertex, partitions);
                out.collect(tuple);
                System.out.println("Added vertex " + currentVertex + " to partition: " + partitions.get(0));
            }
        }
        System.out.println("EDGE: " + currentEdge.getOriginVertex() + " " + currentEdge.getDestinVertex());
    }

    public int choosePartition(int vertexId) {
        Random rand = new Random();
        return rand.nextInt(2);

    }

}

*/
