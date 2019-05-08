/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *//*


package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.jobstatistics.JobVertex;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.System.out;
import static java.lang.System.setOut;

*/
/**
 *
 * Try & Error coding with Share State in Flink
 * @parameters: -- not required --
 *
 *//*


public class ShareStateImpl {

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create Input (fake)
        ArrayList<DestinVertex> colorList = new ArrayList<>();
        ArrayList<OriginVertex> shapeList = new ArrayList<>();
        ArrayList<Edge> itemList = new ArrayList<>();

        // 5 color objects
        for (int i = 0; i < 5; i++) {
            DestinVertex c = new DestinVertex(i);
            colorList.add(c);
        }

        // 10 shape objects
        for (int i = 0; i < 10; i++) {
            OriginVertex s = new OriginVertex(i);
            shapeList.add(s);
        }

        // 20 "random" Items based on color/shape combinations
        int r1; int r2;
        for (int i = 0; i < 20; i++) {
            Random random = new Random();
            r1 = random.nextInt(5);
            r2 = random.nextInt(10);
            Edge item = new Edge(colorList.get(r1), shapeList.get(r2), i);
            itemList.add(item);
        }

        // Create simulated/hardcoded state table
        ArrayList<Rule> ruleList = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            Rule rule = new Rule(i);
            ruleList.add(rule);

        }



        // Get input data
        DataStream<Edge> streamInput = env.fromCollection(itemList);
        DataStream<Rule> ruleStream = env.fromCollection(ruleList);

        // key the shapes by color
        KeyedStream<Edge, DestinVertex> colorPartitionedStream = streamInput.keyBy(Edge::getDestinVertex);
        //keyBy(new KeySelector<OriginVertex, DestinVertex>(){});
        //streamInput.printPhaseOne();

        // a map descriptor to store the name of the rule (string) and the rule itself.
        MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Rule>() {}));

        // broadcast the rules and create the broadcast state
        BroadcastStream<Rule> ruleBroadcastStream = ruleStream
                .broadcast(ruleStateDescriptor);


        DataStream<Match> output = colorPartitionedStream
                .connect(ruleBroadcastStream)
                .process(

                        // type arguments in our KeyedBroadcastProcessFunction represent:
                        //   1. the key of the keyed stream
                        //   2. the type of elements in the non-broadcast side
                        //   3. the type of elements in the broadcast side
                        //   4. the type of the result, here a string
                        //   new KeyedBroadcastProcessFunction<DestinVertex, Edge, Rule, String>() {

            // store partial matches, i.e. first elements of the pair waiting for their second element
            // we keep a list as we may have many first elements waiting
            final MapStateDescriptor<String, List<Edge>> mapStateDesc =
                    new MapStateDescriptor<>(
                            "items",
                            BasicTypeInfo.STRING_TYPE_INFO,
                            new ListTypeInfo<>(Edge.class));

            @Override
            public void processBroadcastElement(Rule value,
                                                Context ctx,
                                                Collector<String> out) throws Exception {
                ctx.getBroadcastState(ruleStateDescriptor).put(value.name, value);
            }

            @Override
            public void processElement(Edge value,
                                       ReadOnlyContext ctx,
                                       Collector<String> out) throws Exception {

                final MapState<String, List<Edge>> state = getRuntimeContext().getMapState(mapStateDesc);
                final OriginVertex shape = value.getOriginVertex();

                for (Map.Entry<String, Rule> entry :
                        ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {
                    final String ruleName = entry.getKey();
                    final Rule rule = entry.getValue();

                    List<Edge> stored = state.get(ruleName);
                    if (stored == null) {
                        stored = new ArrayList<>();
                    }

                    if (shape == rule.second && !stored.isEmpty()) {
                        for (Edge i : stored) {
                            out.collect("MATCH: " + i + " - " + value);
                        }
                        stored.clear();
                    }

                    // there is no else{} to cover if rule.first == rule.second
                    if (shape.equals(rule.first)) {
                        stored.add(value);
                    }

                    if (stored.isEmpty()) {
                        state.remove(ruleName);
                    } else {
                        state.put(ruleName, stored);
                    }
                }
            }
        }





        // Execute program
        JobExecutionResult result = env.execute("Streaming Items and Partitioning");
        long executionTime = result.getNetRuntime();

    }

    public static class PartitionByTag implements Partitioner<Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            return key % numPartitions;
        }
    }

    public class WhatEver {
    }

    public class Match {
    }

}


*/
/*


    // Get fake state table
    DataStream<String> stateStream = env.fromCollection(hardCodedState);

    // Initialize state table (Columns: Vertex, Occurrence)
    HashMap<String, Long> stateTable = new HashMap<>();

    // FlatMap function to create proper tuples (Tuple2) with both vertices, as in input file
    SingleOutputStreamOperator edges = streamInput.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
        @Override public void flatMap(String streamInput, Collector<Tuple2<String, String>> out) {

            // normalize and split the line
            String[] tokens = streamInput.replaceAll("\\s+","").split("\\r?\\n");
            // emit the pairs
            for (String token:tokens) {
                if (token.length() > 0) {
                    String[] elements = token.split(",",2);
                    String t0 = elements[0];
                    String t1 = elements[1];
                    out.collect(new Tuple2<>(t0, t1));
                    Arrays.fill(elements, null);

                }

            }
        }});

    // Tag edges (results from FlatMap) by constructing a state table (hash map) that keeps track of vertices and their occurrences
    SingleOutputStreamOperator taggedEdges = edges.map(new MapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>>() {
        @Override public Tuple3 map(Tuple2<String, String> tuple) throws Exception {
            String[] vertices = new String[2];
            vertices[0] = tuple.f0;
            vertices[1] = tuple.f1;
            Long highest = 0L;

            // Delete if "tag" is used
            int mostFreq = Integer.parseInt(tuple.f0);

            // Tagging for partitions

            Long count;
            // Loop over both vertices and see which one has the higher degree (if equal, the left vertex "wins").
            for (int i = 0; i < 2; i++) {
                if (stateTable.containsKey(vertices[i])) {
                    count = stateTable.get(vertices[i]);
                    count++;
                    stateTable.put(vertices[i], count);
                } else {
                    stateTable.put(vertices[i], 1L);
                    count = 1L;
                }
                if (count > highest) {
                    highest = count;
                    mostFreq = Integer.parseInt(tuple.getField(i));;
                    //tag = (int) tuple.getField(i) % partitions;
                }
            }

            return new Tuple3<>(tuple.f0,tuple.f1,mostFreq);

        }});

    // Partition edges
    int partitions = env.getConfig().getParallelism();

    DataStream partitionedEdges = taggedEdges.partitionCustom(new PartitionByTag(),2);

        partitionedEdges.printPhaseOne();

                // KEY BY (field 1) --> destination vertex
                //KeyedStream<String, String> keyedInput = partitionedEdges.keyBy(1);
                //keyedInput.printPhaseOne();

                MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>
                ("TestBroadCastState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<String>() {}));

        BroadcastStream<String> stateStreamBroad = stateStream.broadcast(stateDescriptor);

        DataStream<Match> output = partitionedEdges.connect(stateStreamBroad).process(
        new KeyedBroadcastProcessFunction<DestinVertex, Edge, Rule, String>() {

// store partial matches, i.e. first elements of the pair waiting for their second element
// we keep a list as we may have many first elements waiting
private final MapStateDescriptor<String, List<Edge>> mapStateDesc =
        new MapStateDescriptor<>(
        "items",
        BasicTypeInfo.STRING_TYPE_INFO,
        new ListTypeInfo<>(Edge.class));

// identical to our ruleStateDescriptor above
private final MapStateDescriptor<String, Rule> ruleStateDescriptor =
        new MapStateDescriptor<>(
        "RulesBroadcastState",
        BasicTypeInfo.STRING_TYPE_INFO,
        TypeInformation.of(new TypeHint<Rule>() {}));

@Override
public void processBroadcastElement(Rule value,
        Context ctx,
        Collector<String> out) throws Exception {
        ctx.getBroadcastState(ruleStateDescriptor).put(value.name, value);
        }

@Override
public void processElement(Edge value,
        ReadOnlyContext ctx,
        Collector<String> out) throws Exception {

final MapState<String, List<Edge>> state = getRuntimeContext().getMapState(mapStateDesc);
final OriginVertex shape = value.getOriginVertex();

        for (Map.Entry<String, Rule> entry :
        ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {
final String ruleName = entry.getKey();
final Rule rule = entry.getValue();

        List<Edge> stored = state.get(ruleName);
        if (stored == null) {
        stored = new ArrayList<>();
        }

        if (shape == rule.second && !stored.isEmpty()) {
        for (Edge i : stored) {
        out.collect("MATCH: " + i + " - " + value);
        }
        stored.clear();
        }

        // there is no else{} to cover if rule.first == rule.second
        if (shape.equals(rule.first)) {
        stored.add(value);
        }

        if (stored.isEmpty()) {
        state.remove(ruleName);
        } else {
        state.put(ruleName, stored);
        }
        }
        }
        }


        /*{
            @Override
            //public void processElement(Object o, ReadOnlyContext readOnlyContext, Collector collector) throws Exception {
            //@Override public void flatMap(String streamInput, Collector<Tuple2<String, String>> out) {
            public void processElement(Object o, ReadOnlyContext readOnlyContext, Collector collector) throws Exception {
                out.println(o);
                collector.collect(new String());
            }

            @Override
            public void processBroadcastElement(Object o, Context context, Collector collector) throws Exception {
                collector.collect(new String());
            }
        });
*//*


*/
