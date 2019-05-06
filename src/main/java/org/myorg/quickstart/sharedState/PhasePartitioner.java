package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class PhasePartitioner {

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    final static Class<Tuple2<Integer, ArrayList<Integer>>> typedTuple = (Class<Tuple2<Integer, ArrayList<Integer>>>) (Class<?>) Tuple2.class;

    final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(
            typedTuple,
            new GenericTypeInfo<>(Integer.class),
            new GenericTypeInfo<>(Integer.class)
    );

    public static void main(String[] args) throws Exception {

        // Argument fetching
        int graphSize = 20;
        int windowSizeInMs = 1000;

        // Environment setup
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // MapState Descriptor (as from data artisans)
        MapStateDescriptor<String, Tuple2<Integer, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO,tupleTypeInfo);

        // Generate a new synthetic graph of edges
        TestingGraph edgeGraph = new TestingGraph("byOrigin",graphSize);
        edgeGraph.printGraph();

        // Create initial EdgeStream
        DataStream<EdgeEvent> edgeSteam = edgeGraph.getEdgeStream(env);

        // *** PHASE 1 ***
        // Process edges to build the local model
        DataStream<HashMap> phaseOneStream = edgeSteam
                .keyBy(new KeySelector<EdgeEvent, Integer>() {
                    @Override
                    public Integer getKey(EdgeEvent value) throws Exception {
                        return value.getEdge().getOriginVertex();
                    }
                })
                .timeWindow(Time.milliseconds(windowSizeInMs))
                .process(new ProcessFirstPhase() {
                });
        //phaseOneStream.print();

        // Process edges in the similar time windows to "wait" for phase 2
        DataStream<EdgeEvent> edgesWindowed = edgeSteam
                .keyBy(new KeySelector<EdgeEvent, Integer>() {
                    @Override
                    public Integer getKey(EdgeEvent value) throws Exception {
                        return value.getEdge().getOriginVertex();
                    }
                })
                .timeWindow(Time.milliseconds(windowSizeInMs))
                //.trigger(CountTrigger.of(1))
                .process(new ProcessWindow() {
                });

        // *** Phase 2 ***
        // Broadcast local state from Phase 1 to all Task Managers
        BroadcastStream<HashMap> broadcastFrequency = phaseOneStream
                .broadcast(rulesStateDescriptor);

        // Connect Broadcast Stream and Edge Stream to build global model
        DataStream<Tuple2<EdgeEvent,Integer>> phaseTwoStream = edgesWindowed
                .keyBy(new KeySelector<EdgeEvent, Integer>() {
                    @Override
                    public Integer getKey(EdgeEvent value) throws Exception {
                        return value.getEdge().getOriginVertex();
                    }
                })
                .connect(broadcastFrequency)
                .process(new MatchFunctionEdges());

        //Print result in human-readable way
             // Tuple3 (Vertex, Vertex, Partition) --> e.g. (4,2,0) is Edge(4,2) located in Parttition 0
 /*       phaseTwoStream.map(new MapFunction<Tuple2<EdgeEvent, Integer>, Tuple3<Integer, Integer, Integer>>() {
            public Tuple3<Integer, Integer, Integer> map(Tuple2<EdgeEvent, Integer> input) {
                return new Tuple3<>(input.f0.getEdge().getOriginVertex(), input.f0.getEdge().getDestinVertex(), input.f1);
            }
        }).print();*/



        // *** Job Analytics



        // ### Finally, execute the job in Flink
        env.execute();

    }
}
