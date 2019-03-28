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
import org.apache.flink.api.java.typeutils.*;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BroadcastStateGraph {

/*    public enum Vertex {
        ONE, TWO, THREE, FOUR, FIVE, SIX
    }*/

    private static class Vertex {
            private final int id;
            public Vertex(int id) {
                this.id = id;
            }
            public int getId() {
                return this.id;
            }
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

    final static Class<Tuple2<Vertex, Long>> typedTuple = (Class<Tuple2<Vertex, Long>>) (Class<?>) Tuple2.class;

    final static TupleTypeInfo<Tuple2<Vertex, Long>> tupleTypeInfo = new TupleTypeInfo<>(
            typedTuple,
            new GenericTypeInfo<>(Vertex.class),
            new GenericTypeInfo<>(Long.class)
    );

    public static void main(String[] args) throws Exception {

        final List<Tuple2<Vertex, Long>> partitionList = new ArrayList<>();
        partitionList.add(new Tuple2<>(new Vertex(1), 1L)); // param1 : vertex ID || param2 : number of occurrences (fake obviously)
        partitionList.add(new Tuple2<>(new Vertex(2), 1L));
        partitionList.add(new Tuple2<>(new Vertex(3), 1L));

        final List<Edge> keyedInput = new ArrayList<>();
        keyedInput.add(new Edge(new Vertex(1),new Vertex(2)));
        keyedInput.add(new Edge(new Vertex(1),new Vertex(3)));
        keyedInput.add(new Edge(new Vertex(1),new Vertex(4)));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MapStateDescriptor<String, Tuple2<Vertex, Long>> rulesStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                tupleTypeInfo
        );


        KeyedStream<Edge, Vertex> edgeKeyedStream = env.fromCollection(keyedInput)
                .rebalance()                               // needed to increase the parallelism
                .map(edge -> edge)
                .setParallelism(4)
                .keyBy(edge -> edge.originVertex);

        BroadcastStream<Tuple2<Vertex, Long>> broadcastStateStream = env.fromCollection(partitionList)
                .flatMap(new FlatMapFunction<Tuple2<Vertex, Long>, Tuple2<Vertex, Long>>() {
                    private static final long serialVersionUID = 6462244253439410814L;

                    @Override
                    public void flatMap(Tuple2<Vertex, Long> value, Collector<Tuple2<Vertex, Long>> out) {
                        // out.collect(value); // OLD CODE
                        out.collect(new Tuple2<>(new Vertex(1), 20L));
                    }
                })
                .setParallelism(4)
                .broadcast(rulesStateDescriptor);

        DataStream<String> output = edgeKeyedStream
                .connect(broadcastStateStream)
                .process(new MatchFunction());

        edgeKeyedStream.print();
        output.print();
        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    public static class MatchFunction extends KeyedBroadcastProcessFunction<Vertex, Edge, Tuple2<Vertex, Long>, String> {

        private int counter = 0;

        private final MapStateDescriptor<String, List<Edge>> matchStateDesc =
                new MapStateDescriptor<>("edges", BasicTypeInfo.STRING_TYPE_INFO, new ListTypeInfo<>(Edge.class));

        private final MapStateDescriptor<String, Tuple2<Vertex, Long>> broadcastStateDescriptor =
                new MapStateDescriptor<>("BroadcastVertexState", BasicTypeInfo.STRING_TYPE_INFO, tupleTypeInfo);

        @Override
        public void processBroadcastElement(Tuple2<Vertex, Long> value, Context ctx, Collector<String> out) throws Exception {
            ctx.getBroadcastState(broadcastStateDescriptor)
                    .put("Rule_" + counter++, value);
            System.out.println("Vertex State_" + counter++ + value);
        }

        @Override
        public void processElement(Edge nextEdge, ReadOnlyContext ctx, Collector<String> out) throws Exception {

            final MapState<String, List<Edge>> partialMatches = getRuntimeContext().getMapState(matchStateDesc);
            final int originVertexOfNextItem = nextEdge.getVertex().getId();

            System.out.println("SAW: " + nextEdge.getVertex().getId());
            for (Map.Entry<String, Tuple2<Vertex, Long>> entry: ctx.getBroadcastState(broadcastStateDescriptor).immutableEntries()) {
                System.out.println("SAW111: " + nextEdge.getVertex().getId());
                final String ruleName = entry.getKey();
                final Tuple2<Vertex, Long> rule = entry.getValue();
                System.out.println("processed");
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

                if (originVertexOfNextItem == rule.f0.getId()) {
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