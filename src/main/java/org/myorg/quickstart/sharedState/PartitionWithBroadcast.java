package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EnumTypeInfo;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PartitionWithBroadcast {

    final static Class<Tuple2<Vertex, ArrayList<Integer>>> typedTuple = (Class<Tuple2<Vertex, ArrayList<Integer>>>) (Class<?>) Tuple2.class;

    final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(
            typedTuple,
            new GenericTypeInfo<>(Vertex.class),
            new GenericTypeInfo<>(Vertex.class)
    );

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Generate "random" edges as input for stream
        List<Edge> keyedInput = new ArrayList<>();
        keyedInput.add(new Edge(new Vertex(1), new Vertex(2)));
        keyedInput.add(new Edge(new Vertex(1), new Vertex(3)));
        keyedInput.add(new Edge(new Vertex(1), new Vertex(4)));
        keyedInput.add(new Edge(new Vertex(1), new Vertex(5)));
        keyedInput.add(new Edge(new Vertex(2), new Vertex(4)));
        keyedInput.add(new Edge(new Vertex(2), new Vertex(6)));

        // create 1 sample "state" for Vertex 1, appearing in partition 1
        List<Integer> stateArray = new ArrayList<>(); stateArray.add(1);
        List<Tuple2<Vertex, List<Integer>>> stateList = new ArrayList<>();
        stateList.add(new Tuple2<>(new Vertex(1), stateArray));

        // MapState Descriptor (as from data artisans)
        MapStateDescriptor<String, Tuple2<Vertex, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                tupleTypeInfo
        );


        KeyedStream<Edge, Vertex> itemColorKeyedStream = env.fromCollection(keyedInput)
                .rebalance()                               // needed to increase the parallelism
                .map(edge -> edge)
                .setParallelism(4)
                .keyBy(Edge::getVertex);

        BroadcastStream<Tuple2<Vertex, List<Integer>>> broadcastRulesStream = env.fromCollection(stateList)
                .flatMap(new FlatMapFunction<Tuple2<Vertex, List<Integer>>, Tuple2<Vertex, List<Integer>>>() {
                    private static final long serialVersionUID = 6462244253439410814L;

                    @Override
                    public void flatMap(Tuple2<Vertex, List<Integer>> value, Collector<Tuple2<Vertex, List<Integer>>> out) {
                        out.collect(value);
                    }
                })
                .setParallelism(4)
                .broadcast(rulesStateDescriptor);

        DataStream<String> output = itemColorKeyedStream
                .connect(broadcastRulesStream)
                .process(new MatchFunction());

        itemColorKeyedStream.print();
        output.print();
        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    public static class MatchFunction extends KeyedBroadcastProcessFunction<Vertex, Edge, Tuple2<Vertex, List<Integer>>, String> {

        private int counter = 0;

        private final MapStateDescriptor<String, List<Edge>> matchStateDesc =
                new MapStateDescriptor<>("edges", BasicTypeInfo.STRING_TYPE_INFO, new ListTypeInfo<>(Edge.class));

        private final MapStateDescriptor<String, Tuple2<Vertex, List<Integer>>> broadcastStateDescriptor =
                new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, tupleTypeInfo);

        @Override
        public void processBroadcastElement(Tuple2<Vertex, List<Integer>> value, Context ctx, Collector<String> out) throws Exception {
            ctx.getBroadcastState(broadcastStateDescriptor).put("Rule_" + counter++, value);
            System.out.println("ADDED: Rule_" + (counter-1) + " " + value);
        }

        @Override
        public void processElement(Edge nextEdge, ReadOnlyContext ctx, Collector<String> out) throws Exception {

            final MapState<String, List<Edge>> partialMatches = getRuntimeContext().getMapState(matchStateDesc);
            final Vertex originVertexOfNextItem = nextEdge.getVertex();

            System.out.println("SAW: " + nextEdge.getVertex().getId());
            for (Map.Entry<String, Tuple2<Vertex, List<Integer>>> entry: ctx.getBroadcastState(broadcastStateDescriptor).immutableEntries()) {
                final String ruleName = entry.getKey();
                final Tuple2<Vertex, List<Integer>> state = entry.getValue();

                List<Edge> partialsForThisRule = partialMatches.get(ruleName);
                if (partialsForThisRule == null) {
                    partialsForThisRule = new ArrayList<>();
                }

                if (originVertexOfNextItem == state.f0 && !partialsForThisRule.isEmpty()) {
                    for (Edge i : partialsForThisRule) {
                        out.collect("MATCH: " + i + " - " + nextEdge);
                    }
                    partialsForThisRule.clear();
                }

                if (originVertexOfNextItem == state.f0) {
                    partialsForThisRule.add(nextEdge);
                }

                if (partialsForThisRule.isEmpty()) {
                    partialMatches.remove(ruleName);
                } else {
                    partialMatches.put(ruleName, partialsForThisRule);
                }
            }
        }
    }
}