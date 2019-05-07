package org.myorg.quickstart.sharedState.old;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.myorg.quickstart.sharedState.Vertex;

import java.util.ArrayList;
import java.util.List;

public class BroadcastPartitioner {

    final static Class<Tuple2<Vertex, ArrayList<Integer>>> typedTuple = (Class<Tuple2<Vertex, ArrayList<Integer>>>) (Class<?>) Tuple2.class;

    final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(
            typedTuple,
            new GenericTypeInfo<>(Vertex.class),
            new GenericTypeInfo<>(Vertex.class)
    );

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // Generate "random" edges as input for stream
        List<Edge> keyedInput = getGraph();

        // MapState Descriptor (as from data artisans)
        MapStateDescriptor<String, Tuple2<Vertex, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                tupleTypeInfo
        );

        final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

        MatchFunction matchFunction = new MatchFunction();

        // create 1 sample "state" for Vertex 1, appearing in partition 1
        List<Integer> stateArray = new ArrayList<>(); stateArray.add(1); stateArray.add(3);
        List<Tuple2<Vertex, List<Integer>>> stateList = new ArrayList<>();
        stateList.add(new Tuple2<>(new Vertex(1), stateArray));
        // 2nd state Array for other vertex
        List<Integer> stateArray1 = new ArrayList<>();stateArray1.add(3); stateArray1.add(4);

        //System.out.println("iteration: " + i + " :: stateList size: " + stateList.size());

        // Stream of state table, based on an ArrayList
        BroadcastStream<Tuple2<Vertex, List<Integer>>> broadcastRulesStream = env.fromCollection(stateList)
                .flatMap(new FlatMapFunction<Tuple2<Vertex, List<Integer>>, Tuple2<Vertex, List<Integer>>>() {
                    @Override
                    public void flatMap(Tuple2<Vertex, List<Integer>> value, Collector<Tuple2<Vertex, List<Integer>>> out) {
                        out.collect(value);
                    }
                })
                .setParallelism(2)
                .broadcast(rulesStateDescriptor);

        // Stream of with window-sized amount of edges
        KeyedStream<Edge, Vertex> edgeKeyedStream = env.fromCollection(keyedInput.subList(0,4))
                .rebalance()                               // needed to increase the parallelism
                .map(edge -> edge)
                .setParallelism(2)
                .keyBy(Edge::getOriginVertex);

        DataStream<String> output = edgeKeyedStream
                .connect(broadcastRulesStream)
                .process(matchFunction);

        DataStream<String> test = edgeKeyedStream
                .connect(broadcastRulesStream)
                .process(matchFunction);


        //DataStream<String> sideOutputStream = output.getSideOutput(outputTag);
        output.print();


        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    // Get the graph to stream
    public static List<Edge> getGraph() {
        List<Edge> keyedInput = new ArrayList<>();
        keyedInput.add(new Edge(new Vertex(1), new Vertex(2)));
        keyedInput.add(new Edge(new Vertex(1), new Vertex(3)));
        keyedInput.add(new Edge(new Vertex(1), new Vertex(4)));
        keyedInput.add(new Edge(new Vertex(1), new Vertex(5)));
        keyedInput.add(new Edge(new Vertex(2), new Vertex(3)));
        keyedInput.add(new Edge(new Vertex(2), new Vertex(4)));
        keyedInput.add(new Edge(new Vertex(2), new Vertex(5)));
        keyedInput.add(new Edge(new Vertex(2), new Vertex(6)));
        keyedInput.add(new Edge(new Vertex(3), new Vertex(4)));
        keyedInput.add(new Edge(new Vertex(3), new Vertex(5)));
        keyedInput.add(new Edge(new Vertex(3), new Vertex(6)));
        keyedInput.add(new Edge(new Vertex(3), new Vertex(7)));
        keyedInput.add(new Edge(new Vertex(4), new Vertex(5)));
        keyedInput.add(new Edge(new Vertex(4), new Vertex(6)));
        keyedInput.add(new Edge(new Vertex(4), new Vertex(7)));
        keyedInput.add(new Edge(new Vertex(4), new Vertex(8)));

        return keyedInput;
    }

}