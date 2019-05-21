package org.myorg.quickstart.deprecated.old;

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
import org.myorg.quickstart.deprecated.VertexDepr;

import java.util.ArrayList;
import java.util.List;

public class PartitionWithBroadcast {

    final static Class<Tuple2<VertexDepr, ArrayList<Integer>>> typedTuple = (Class<Tuple2<VertexDepr, ArrayList<Integer>>>) (Class<?>) Tuple2.class;

    final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(
            typedTuple,
            new GenericTypeInfo<>(VertexDepr.class),
            new GenericTypeInfo<>(VertexDepr.class)
    );

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // Generate "random" edges as input for stream
        List<EdgeDepr> keyedInput = getGraph();

        // create 1 sample "state" for VertexDepr 1, appearing in partition 1
        List<Integer> stateArray = new ArrayList<>(); stateArray.add(1); stateArray.add(3);
        List<Tuple2<VertexDepr, List<Integer>>> stateList = new ArrayList<>();
        stateList.add(new Tuple2<>(new VertexDepr(1), stateArray));
        // 2nd state Array for other vertex
        List<Integer> stateArray1 = new ArrayList<>();stateArray1.add(3); stateArray1.add(4);

        // MapState Descriptor (as from data artisans)
        MapStateDescriptor<String, Tuple2<VertexDepr, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                tupleTypeInfo
        );

        MatchFunction matchFunction = new MatchFunction();

        // "Window" quick'n dirty
        int windowSize = 4;
        for (int i = 0; i < windowSize; i++) {


            //System.out.println("iteration: " + i + " :: stateList size: " + stateList.size());

            // Stream of state table, based on an ArrayList
            BroadcastStream<Tuple2<VertexDepr, List<Integer>>> broadcastRulesStream = env.fromCollection(stateList)
                    .flatMap(new FlatMapFunction<Tuple2<VertexDepr, List<Integer>>, Tuple2<VertexDepr, List<Integer>>>() {
                        @Override
                        public void flatMap(Tuple2<VertexDepr, List<Integer>> value, Collector<Tuple2<VertexDepr, List<Integer>>> out) {
                            out.collect(value);
                        }
                    })
                    .setParallelism(2)
                    .broadcast(rulesStateDescriptor);

            // Stream of with window-sized amount of edges
            KeyedStream<EdgeDepr, VertexDepr> edgeKeyedStream = env.fromCollection(keyedInput.subList(i*windowSize,i*windowSize+windowSize))
                    .rebalance()                               // needed to increase the parallelism
                    .map(edge -> edge)
                    .setParallelism(2)
                    .keyBy(EdgeDepr::getOriginVertexDepr);

            DataStream<String> output = edgeKeyedStream
                    .connect(broadcastRulesStream)
                    .process(matchFunction);

            //output.broadcast(rulesStateDescriptor);


            output.print();
        } // for loop

        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    // Get the graph to stream
    public static List<EdgeDepr> getGraph() {
        List<EdgeDepr> keyedInput = new ArrayList<>();
        keyedInput.add(new EdgeDepr(new VertexDepr(1), new VertexDepr(2)));
        keyedInput.add(new EdgeDepr(new VertexDepr(1), new VertexDepr(3)));
        keyedInput.add(new EdgeDepr(new VertexDepr(1), new VertexDepr(4)));
        keyedInput.add(new EdgeDepr(new VertexDepr(1), new VertexDepr(5)));
        keyedInput.add(new EdgeDepr(new VertexDepr(2), new VertexDepr(3)));
        keyedInput.add(new EdgeDepr(new VertexDepr(2), new VertexDepr(4)));
        keyedInput.add(new EdgeDepr(new VertexDepr(2), new VertexDepr(5)));
        keyedInput.add(new EdgeDepr(new VertexDepr(2), new VertexDepr(6)));
        keyedInput.add(new EdgeDepr(new VertexDepr(3), new VertexDepr(4)));
        keyedInput.add(new EdgeDepr(new VertexDepr(3), new VertexDepr(5)));
        keyedInput.add(new EdgeDepr(new VertexDepr(3), new VertexDepr(6)));
        keyedInput.add(new EdgeDepr(new VertexDepr(3), new VertexDepr(7)));
        keyedInput.add(new EdgeDepr(new VertexDepr(4), new VertexDepr(5)));
        keyedInput.add(new EdgeDepr(new VertexDepr(4), new VertexDepr(6)));
        keyedInput.add(new EdgeDepr(new VertexDepr(4), new VertexDepr(7)));
        keyedInput.add(new EdgeDepr(new VertexDepr(4), new VertexDepr(8)));

        return keyedInput;
    }

}