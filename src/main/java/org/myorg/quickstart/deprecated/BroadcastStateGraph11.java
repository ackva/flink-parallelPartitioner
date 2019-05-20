package org.myorg.quickstart.deprecated;

/*
 * Copyright 2018 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EnumTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BroadcastStateGraph11 {

    public enum Vertex {
        ONE, TWO, THREE, FOUR, FIVE, SIX
    }

    private static class Edge {

        private final Vertex originVertex;
        private final Vertex destinVertex;

        Edge(final Vertex originVertex, final Vertex destinVertex) {
            this.destinVertex = destinVertex;
            this.originVertex = originVertex;
        }

        public Vertex getVertex() {
            return destinVertex;
        }

        @Override
        public String toString() {
            return "Edge{" +
                    "originVertex=" + originVertex +
                    ", destinVertex=" + destinVertex +
                    '}';
        }
    }
    
    final static Class<Tuple2<Vertex, Vertex>> typedTuple = (Class<Tuple2<Vertex, Vertex>>) (Class<?>) Tuple2.class;

    final static TupleTypeInfo<Tuple2<Vertex, Vertex>> tupleTypeInfo = new TupleTypeInfo<>(
            typedTuple,
            new EnumTypeInfo<>(Vertex.class),
            new EnumTypeInfo<>(Vertex.class)
    );

    public static void main(String[] args) throws Exception {

        final List<Tuple2<Vertex, Vertex>> rules = new ArrayList<>();
        rules.add(new Tuple2<>(Vertex.ONE, Vertex.TWO));

        final List<Edge> keyedInput = new ArrayList<>();
        keyedInput.add(new Edge(Vertex.ONE, Vertex.FIVE));
        keyedInput.add(new Edge(Vertex.TWO, Vertex.SIX));
        keyedInput.add(new Edge(Vertex.TWO, Vertex.FOUR));
        keyedInput.add(new Edge(Vertex.THREE, Vertex.SIX));
        keyedInput.add(new Edge(Vertex.THREE, Vertex.FIVE));
        keyedInput.add(new Edge(Vertex.TWO, Vertex.SIX));
        keyedInput.add(new Edge(Vertex.ONE, Vertex.FIVE));
        keyedInput.add(new Edge(Vertex.THREE, Vertex.FIVE));
        keyedInput.add(new Edge(Vertex.TWO, Vertex.FIVE));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MapStateDescriptor<String, Tuple2<Vertex, Vertex>> rulesStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                tupleTypeInfo
        );


        KeyedStream<Edge, Vertex> itemColorKeyedStream = env.fromCollection(keyedInput)
                .rebalance()                               // needed to increase the parallelism
                .map(edge -> edge)
                .setParallelism(4)
                .keyBy(edge -> edge.destinVertex);

        BroadcastStream<Tuple2<Vertex, Vertex>> broadcastRulesStream = env.fromCollection(rules)
                .flatMap(new FlatMapFunction<Tuple2<Vertex, Vertex>, Tuple2<Vertex, Vertex>>() {
                    private static final long serialVersionUID = 6462244253439410814L;

                    @Override
                    public void flatMap(Tuple2<Vertex, Vertex> value, Collector<Tuple2<Vertex, Vertex>> out) {
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

    public static class MatchFunction extends KeyedBroadcastProcessFunction<Vertex, Edge, Tuple2<Vertex, Vertex>, String> {

        private int counter = 0;

        private final MapStateDescriptor<String, List<Edge>> matchStateDesc =
                new MapStateDescriptor<>("items", BasicTypeInfo.STRING_TYPE_INFO, new ListTypeInfo<>(Edge.class));

        private final MapStateDescriptor<String, Tuple2<Vertex, Vertex>> broadcastStateDescriptor =
                new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, tupleTypeInfo);

        @Override
        public void processBroadcastElement(Tuple2<Vertex, Vertex> value, Context ctx, Collector<String> out) throws Exception {
            ctx.getBroadcastState(broadcastStateDescriptor).put("Rule_" + counter++, value);
            System.out.println("ADDED: Rule_" + (counter-1) + " " + value);
        }

        @Override
        public void processElement(Edge nextEdge, ReadOnlyContext ctx, Collector<String> out) throws Exception {

            final MapState<String, List<Edge>> partialMatches = getRuntimeContext().getMapState(matchStateDesc);
            final Vertex originVertexOfNextItem = nextEdge.getVertex();

            System.out.println("SAW: " + nextEdge);
            for (Map.Entry<String, Tuple2<Vertex, Vertex>> entry: ctx.getBroadcastState(broadcastStateDescriptor).immutableEntries()) {
                final String ruleName = entry.getKey();
                final Tuple2<Vertex, Vertex> rule = entry.getValue();

                List<Edge> partialsForThisRule = partialMatches.get(ruleName);
                if (partialsForThisRule == null) {
                    partialsForThisRule = new ArrayList<>();
                }

                if (originVertexOfNextItem == rule.f1 && !partialsForThisRule.isEmpty()) {
                    for (Edge i : partialsForThisRule) {
                        out.collect("MATCH: " + i + " - " + nextEdge);
                    }
                    partialsForThisRule.clear();
                }

                if (originVertexOfNextItem == rule.f0) {
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