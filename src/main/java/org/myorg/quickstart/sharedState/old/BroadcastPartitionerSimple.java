package org.myorg.quickstart.sharedState.old;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.sharedState.EdgeSimple;
import org.myorg.quickstart.sharedState.TestingGraph;

import java.util.*;

public class BroadcastPartitionerSimple {

    final static Class<Tuple2<Integer, ArrayList<Integer>>> typedTuple = (Class<Tuple2<Integer, ArrayList<Integer>>>) (Class<?>) Tuple2.class;

    final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(
            typedTuple,
            new GenericTypeInfo<>(Integer.class),
            new GenericTypeInfo<>(Integer.class)
    );

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // Generate a graph
        TestingGraph graph = new TestingGraph();
        graph.generateGraphOneToAny(15);
        List<EdgeSimple> keyedInput = graph.getEdges();
        graph.printGraph();

        // MapState Descriptor (as from data artisans)
        MapStateDescriptor<String, Tuple2<Integer, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                tupleTypeInfo
        );

        MatchFunctionRule matchRules = new MatchFunctionRule();
        matchRules.setRound(1);

        // create 1 sample "state" for Vertex 1, appearing in partition 1
        List<Integer> stateArray = new ArrayList<>(); stateArray.add(-1); stateArray.add(-1);
        List<Tuple2<Integer, List<Integer>>> stateList = new ArrayList<>();
        stateList.add(new Tuple2<>(new Integer(-1), stateArray));


        // ##### ROUND 1 #####
        // Stream of state table, based on an ArrayList
        BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream = env.fromCollection(stateList)
                .flatMap(new FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>>() {
                    @Override
                    public void flatMap(Tuple2<Integer, List<Integer>> value, Collector<Tuple2<Integer, List<Integer>>> out) {
                        out.collect(value);
                    }
                })
                .setParallelism(2)
                .broadcast(rulesStateDescriptor);

        // Stream of with window-sized amount of edges
        KeyedStream<EdgeSimple, Integer> edgeKeyedStream = env.fromCollection(keyedInput.subList(0,5))
                //.rebalance()                               // needed to increase the parallelism
                .map(edgeSimple -> edgeSimple)
                .setParallelism(2)
                .keyBy(EdgeSimple::getOriginVertex);

        edgeKeyedStream.print();

        DataStream<Tuple2<Integer, List<Integer>>> outputRules = edgeKeyedStream
                .connect(broadcastRulesStream)
                .process(matchRules);

        //outputRules.printPhaseOne();

/*
       KeySelector abc = new KeySelector<Tuple2<Integer, List<Integer>>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, List<Integer>> value) throws Exception {
                return value.f0;
            }
        };
        //Tuple2<Integer, List<Integer>> value


        DataStream<Tuple2<Integer, List<Integer>>> streamOutput = DataStreamUtils
                .reinterpretAsKeyedStream(outputRules, abc);
*/


        // ##### ROUND 1 #####
        MatchFunctionRule matchRules2 = new MatchFunctionRule();
        matchRules2.setRound(2);


        BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream2 = outputRules
        //BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream2 = outputRules
            .flatMap(new FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>>() {
                @Override
                public void flatMap(Tuple2<Integer, List<Integer>> value, Collector<Tuple2<Integer, List<Integer>>> out) {
                    out.collect(value);
                }
            })
            .setParallelism(2)
            .broadcast(rulesStateDescriptor);

        KeyedStream<EdgeSimple, Integer> edgeKeyedStream2 = env.fromCollection(keyedInput.subList(5,10))
            .rebalance()                               // needed to increase the parallelism
            .map(edgeSimple -> edgeSimple)
            .setParallelism(2)
            .keyBy(EdgeSimple::getOriginVertex);

        DataStream<Tuple2<Integer, List<Integer>>> outputRules2 = edgeKeyedStream2
            .connect(broadcastRulesStream2)
            .process(matchRules2);

        //outputRules2.printPhaseOne();

/*        DataStream<Tuple2<Integer, List<Integer>>> streamOutput2 = DataStreamUtils
                .reinterpretAsKeyedStream(outputRules2, abc);*/


        // ##### ROUND 3 #####
        MatchFunctionRule matchRules3 = new MatchFunctionRule();
        matchRules3.setRound(3);

        BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream3 = outputRules2
            .flatMap(new FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>>() {
                @Override
                public void flatMap(Tuple2<Integer, List<Integer>> value, Collector<Tuple2<Integer, List<Integer>>> out) {
                    out.collect(value);
                }
            })
            .setParallelism(2)
            .broadcast(rulesStateDescriptor);

        KeyedStream<EdgeSimple, Integer> edgeKeyedStream3 = env.fromCollection(keyedInput.subList(10,15))
            .rebalance()                               // needed to increase the parallelism
            .map(edgeSimple -> edgeSimple)
            .setParallelism(2)
            .keyBy(EdgeSimple::getOriginVertex);

        DataStream<Tuple2<Integer, List<Integer>>> outputRules3 = edgeKeyedStream3
            .connect(broadcastRulesStream3)
            .process(matchRules3);

        //outputRules3.printPhaseOne();


        //System.out.println(env.getExecutionPlan());
        env.execute();
    }


}


 /*
        List<Tuple2<Integer, List<Integer>>> stateList2 = new ArrayList<>();
        stateList2.add(new Tuple2<>(new Integer(-1), stateArray));

        BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream2 = env.fromCollection(stateList2)
                .flatMap(new FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>>() {
                    @Override
                    public void flatMap(Tuple2<Integer, List<Integer>> value, Collector<Tuple2<Integer, List<Integer>>> out) {
                        out.collect(value);
                    }
                 })
                .setParallelism(2)
                .broadcast(rulesStateDescriptor);

        // Stream of with window-sized amount of edges
        KeyedStream<EdgeSimple, Integer> edgeKeyedStream2 = env.fromCollection(keyedInput.subList(5,9))
                .rebalance()                               // needed to increase the parallelism
                .map(edgeSimple -> edgeSimple)
                .setParallelism(2)
                .keyBy(EdgeSimple::getOriginVertex);

        DataStream<String> output2 = edgeKeyedStream2
                .connect(broadcastRulesStream2)
                .process(matchFunction);

        //DataStream<String> sideOutputStream = output.getSideOutput(outputTag);
        //output.printPhaseOne();
        outputRules.printPhaseOne();*/