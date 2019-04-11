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
        KeyedStream<EdgeSimple, Integer> edgeKeyedStream = env.fromCollection(keyedInput.subList(0,4))
                .rebalance()                               // needed to increase the parallelism
                .map(edgeSimple -> edgeSimple)
                .setParallelism(2)
                .keyBy(EdgeSimple::getOriginVertex);

        DataStream<Tuple2<Integer, List<Integer>>> outputRules = edgeKeyedStream
                .connect(broadcastRulesStream)
                .process(matchRules);


        // ##### ROUND 2 #####


        MatchFunctionRule matchRules2 = new MatchFunctionRule();

        BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream2 = outputRules
                .flatMap(new FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>>() {
                    @Override
                    public void flatMap(Tuple2<Integer, List<Integer>> value, Collector<Tuple2<Integer, List<Integer>>> out) {
                        out.collect(value);
                    }
                })
                .setParallelism(2)
                .broadcast(rulesStateDescriptor);

        KeyedStream<EdgeSimple, Integer> edgeKeyedStream2 = env.fromCollection(keyedInput.subList(5,9))
                .rebalance()                               // needed to increase the parallelism
                .map(edgeSimple -> edgeSimple)
                .setParallelism(2)
                .keyBy(EdgeSimple::getOriginVertex);

        DataStream<Tuple2<Integer, List<Integer>>> outputRules2 = edgeKeyedStream2
                .connect(broadcastRulesStream2)
                .process(matchRules2);

        outputRules2.print();

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
        keyedInput.add(new EdgeSimple(2,3));
        keyedInput.add(new EdgeSimple(2,4));
        keyedInput.add(new EdgeSimple(2,5));
        keyedInput.add(new EdgeSimple(2,6));
        keyedInput.add(new EdgeSimple(3,4));
        keyedInput.add(new EdgeSimple(3,5));
        keyedInput.add(new EdgeSimple(3,6));
        keyedInput.add(new EdgeSimple(3,7));
        keyedInput.add(new EdgeSimple(4,5));
        keyedInput.add(new EdgeSimple(4,6));
        keyedInput.add(new EdgeSimple(4,7));
        keyedInput.add(new EdgeSimple(4,8));

        return keyedInput;
    }

}