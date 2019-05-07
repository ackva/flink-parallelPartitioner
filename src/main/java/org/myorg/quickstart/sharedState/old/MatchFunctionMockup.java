package org.myorg.quickstart.sharedState.old;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.sharedState.EdgeSimple;

import java.util.List;
import java.util.Random;

import static org.myorg.quickstart.sharedState.old.PartitionWithBroadcast.tupleTypeInfo;

public class MatchFunctionMockup extends KeyedBroadcastProcessFunction<Integer, Integer, Tuple2<Integer, List<Integer>>, Integer> {

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
    public void processBroadcastElement(Tuple2<Integer, List<Integer>> broadcastElement, Context ctx, Collector<Integer> out) throws Exception {

        System.out.println("R_"+ round + ":: Broadcasting Vertex " + broadcastElement.f0);

    }

    @Override
    public void processElement(Integer currentEdge, ReadOnlyContext ctx, Collector<Integer> out) throws Exception {

        System.out.println("R_"+ round +":: Processing EDGE: " + currentEdge);

                out.collect(currentEdge);
                //System.out.println("Added vertex " + currentVertex + " to partition: " + partitions.get(0));
        }
        //broadcastRules(currentEdge);


    public int choosePartition(int vertexId) {
        Random rand = new Random();
        return rand.nextInt(2);

    }

    public void broadcastRules(EdgeSimple currentEdge) throws Exception {

        System.out.println("broadcastFunction called for " + currentEdge.getOriginVertex() + " " + currentEdge.getDestinVertex());


    }

}

