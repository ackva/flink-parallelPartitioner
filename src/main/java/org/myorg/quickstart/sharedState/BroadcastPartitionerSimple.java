package org.myorg.quickstart.sharedState;

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
import org.apache.flink.util.OutputTag;
import scala.Int;

import java.security.Key;
import java.util.ArrayList;
import java.util.List;

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

        // Generate "random" edges as input for stream
        List<EdgeSimple> keyedInput = getGraph();

        // MapState Descriptor (as from data artisans)
        MapStateDescriptor<String, Tuple2<Integer, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                tupleTypeInfo
        );

        //MatchFunctionSimple matchFunction = new MatchFunctionSimple();
        MatchFunctionRule matchRules = new MatchFunctionRule();

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
        KeyedStream<EdgeSimple, Integer> edgeKeyedStream = env.fromCollection(keyedInput.subList(0,9))
                .rebalance()                               // needed to increase the parallelism
                .map(edgeSimple -> edgeSimple)
                .setParallelism(2)
                .keyBy(EdgeSimple::getOriginVertex);

        DataStream<Tuple2<Integer, List<Integer>>> outputRules = edgeKeyedStream
                .connect(broadcastRulesStream)
                .process(matchRules);

        outputRules.print();


        // ##### ROUND 2 #####

        BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream2 = outputRules
                .flatMap(new FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>>() {
                    @Override
                    public void flatMap(Tuple2<Integer, List<Integer>> value, Collector<Tuple2<Integer, List<Integer>>> out) {
                        out.collect(value);
                    }
                })
                .setParallelism(2)
                .broadcast(rulesStateDescriptor);

        KeyedStream<EdgeSimple, Integer> edgeKeyedStream2 = env.fromCollection(keyedInput.subList(10,19))
                .rebalance()                               // needed to increase the parallelism
                .map(edgeSimple -> edgeSimple)
                .setParallelism(2)
                .keyBy(EdgeSimple::getOriginVertex);

        DataStream<Tuple2<Integer, List<Integer>>> outputRules2 = edgeKeyedStream2
                .connect(broadcastRulesStream2)
                .process(matchRules);

        outputRules2.print();


/*
        // ##### ROUND 3 #####

        BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream3 = outputRules2
                .flatMap(new FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>>() {
                    @Override
                    public void flatMap(Tuple2<Integer, List<Integer>> value, Collector<Tuple2<Integer, List<Integer>>> out) {
                        out.collect(value);
                    }
                })
                .setParallelism(2)
                .broadcast(rulesStateDescriptor);

        KeyedStream<EdgeSimple, Integer> edgeKeyedStream3 = env.fromCollection(keyedInput.subList(20,29))
                .rebalance()                               // needed to increase the parallelism
                .map(edgeSimple -> edgeSimple)
                .setParallelism(2)
                .keyBy(EdgeSimple::getOriginVertex);

        DataStream<Tuple2<Integer, List<Integer>>> outputRules3 = edgeKeyedStream3
                .connect(broadcastRulesStream3)
                .process(matchRules);

        outputRules3.print();
*/




        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    // Get the graph to stream
    public static List<EdgeSimple> getGraph() {

        List<EdgeSimple> keyedInput = new ArrayList<>();
        keyedInput.add(new EdgeSimple(1,2));
        keyedInput.add(new EdgeSimple(1,3));
        keyedInput.add(new EdgeSimple(1,4));
        keyedInput.add(new EdgeSimple(1,5));
        keyedInput.add(new EdgeSimple(1,6));
        keyedInput.add(new EdgeSimple(1,7));
        keyedInput.add(new EdgeSimple(1,8));
        keyedInput.add(new EdgeSimple(1,9));
        keyedInput.add(new EdgeSimple(1,10));
        keyedInput.add(new EdgeSimple(1,11));
        keyedInput.add(new EdgeSimple(1,12));
        keyedInput.add(new EdgeSimple(1,13));
        keyedInput.add(new EdgeSimple(1,14));
        keyedInput.add(new EdgeSimple(1,15));
        keyedInput.add(new EdgeSimple(1,16));
        keyedInput.add(new EdgeSimple(1,17));
        keyedInput.add(new EdgeSimple(1,18));
        keyedInput.add(new EdgeSimple(1,19));
        keyedInput.add(new EdgeSimple(1,20));
        keyedInput.add(new EdgeSimple(1,21));
        keyedInput.add(new EdgeSimple(1,22));
        keyedInput.add(new EdgeSimple(1,23));
        keyedInput.add(new EdgeSimple(1,24));
        keyedInput.add(new EdgeSimple(1,25));
        keyedInput.add(new EdgeSimple(1,26));
        keyedInput.add(new EdgeSimple(1,27));
        keyedInput.add(new EdgeSimple(1,28));
        keyedInput.add(new EdgeSimple(1,29));
        keyedInput.add(new EdgeSimple(1,30));
        keyedInput.add(new EdgeSimple(1,31));
        keyedInput.add(new EdgeSimple(1,32));
        keyedInput.add(new EdgeSimple(1,33));

        return keyedInput;
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
        //output.print();
        outputRules.print();*/