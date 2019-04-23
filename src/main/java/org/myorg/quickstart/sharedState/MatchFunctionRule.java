package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import java.util.*;
import static org.myorg.quickstart.sharedState.PartitionWithBroadcast.tupleTypeInfo;

public class MatchFunctionRule extends KeyedBroadcastProcessFunction<Integer, EdgeSimple, Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>> {

    private int counter = 0;
    private String processedEdges = "Edges processed by: ";
    private String processedBroadcastElements = "BroadcastRules processed by: ";
    private int round;

    public void setRound(int round) {
        this.round = round;
    }

    private final MapStateDescriptor<String, Tuple2<Integer, List<Integer>>> broadcastStateDescriptor =
            new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, tupleTypeInfo);

    @Override
    public void processBroadcastElement(Tuple2<Integer, List<Integer>> broadcastElement, Context ctx, Collector<Tuple2<Integer, List<Integer>>> out) throws Exception {

        //System.out.println("R_"+ round + ":: Broadcasting Vertex " + broadcastElement.f0);

        if (broadcastElement.f0 != -1) {
            boolean inList = false;
            //ctx.getBroadcastState(broadcastStateDescriptor).put("Entry_" + counter++, broadcastElement);
            for (Map.Entry<String, Tuple2<Integer, List<Integer>>> stateEntry : ctx.getBroadcastState(broadcastStateDescriptor).entries()) {
                if (stateEntry.getValue().f0 == broadcastElement.f0) {
                    inList = true;
                }

            }
            if (inList == false) {
                //ctx.getBroadcastState(broadcastStateDescriptor).put("Entry_" + counter++, broadcastElement);
                ctx.getBroadcastState(broadcastStateDescriptor).put(broadcastElement.f0.toString(), broadcastElement);
                //System.out.println("R_"+ round +":: RULE: Vertex " + broadcastElement.f0 + " to state table");
                counter++;
                System.out.println("R_"+ round +":: State Table after ADDING Vertex " + broadcastElement.f0 + " --> " + ctx.getBroadcastState(broadcastStateDescriptor).entries());
            }
        }

    }

    @Override
    public void processElement(EdgeSimple currentEdge, ReadOnlyContext ctx, Collector<Tuple2<Integer, List<Integer>>> out) throws Exception {

        //System.out.println("R_"+ round +":: Processing EDGE: " + currentEdge.getOriginVertex() + " " + currentEdge.getDestinVertex());
        processedEdges = processedEdges + currentEdge.getOriginVertex() + "," + currentEdge.getDestinVertex() + "--";
        System.out.println("R_"+ round +":: " + processedEdges);


        List<Integer> currentPartitions = new ArrayList<>();
        boolean addToList;

        // Iterate through both vertices of the current edge and compare it with the existing entries from the state table
        for (Integer currentVertex: currentEdge.getVertices()) {
            addToList = true;
            stateLoop:
            for (Map.Entry<String, Tuple2<Integer, List<Integer>>> stateEntry: ctx.getBroadcastState(broadcastStateDescriptor).immutableEntries()) {
                //System.out.println(stateEntry.getValue().f0 + " < stateF0 --- currentVertex > " + currentVertex);
                if (stateEntry.getValue().f0 == currentVertex) {
                    System.out.println("R_"+ round +":: current vertex: " + currentVertex);
                    addToList = false;
                    break stateLoop;
                }
            }
            if (addToList == true) {
                List<Integer> partitions = new ArrayList<>();
                partitions.add(choosePartition(currentVertex));
                Tuple2<Integer, List<Integer>> tuple = new Tuple2(currentVertex, partitions);
                out.collect(tuple);
                //System.out.println("Added vertex " + currentVertex + " to partition: " + partitions.get(0));
            }
        }

    }

    public int choosePartition(int vertexId) {
        Random rand = new Random();
        return rand.nextInt(2);

    }

}

