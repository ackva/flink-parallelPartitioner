package org.myorg.quickstart.deprecated;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * This is the version that "works" without arguments, etc.
 * Most options (algo, graphSize, printLevel, etc.) are semi-hardcoded.
 *
 */
public class PhasePartitionerBasic {

    public static final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final static Class<Tuple2<Integer, ArrayList<Integer>>> typedTuple = (Class<Tuple2<Integer, ArrayList<Integer>>>) (Class<?>) Tuple2.class;
    final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(typedTuple,new GenericTypeInfo<>(Integer.class),new GenericTypeInfo<>(Integer.class));

    // Static variables for debugging, testing, etc.
    public static long windowSizeInMs = 2000;
    public static long sleep = windowSizeInMs/100;

    public static void main(String[] args) throws Exception {

        // Argument fetching
        int graphSize = 20;

        // Environment setup
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // MapState Descriptor (as from data artisans)
        MapStateDescriptor<String, Tuple2<Integer, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO,tupleTypeInfo);

        // Generate a new synthetic graph of edges
        GraphCreator edgeGraph = new GraphCreator("byOrigin",graphSize);
        edgeGraph.printGraph();

        // Create initial EdgeStream
        DataStream<EdgeEventDepr> edgeSteam = edgeGraph.getEdgeStream(env);

        // *** PHASE 1 ***
        // Process edges to build the local model
        DataStream<HashMap> phaseOneStream = edgeSteam
                .keyBy(new KeySelector<EdgeEventDepr, Integer>() {
                    @Override
                    public Integer getKey(EdgeEventDepr value) throws Exception {
                        return value.getEdge().getOriginVertex();
                    }
                })
                .timeWindow(Time.milliseconds(windowSizeInMs))
                .process(new ProcessFirstPhase() {
                });
        //phaseOneStream.print();

        // Process edges in the similar time windows to "wait" for phase 2
        DataStream<EdgeEventDepr> edgesWindowed = edgeSteam
                .keyBy(new KeySelector<EdgeEventDepr, Integer>() {
                    @Override
                    public Integer getKey(EdgeEventDepr value) throws Exception {
                        return value.getEdge().getOriginVertex();
                    }
                })
                .timeWindow(Time.milliseconds(windowSizeInMs))
                .process(new ProcessWindow() {
                });

        // *** Phase 2 ***
        // Broadcast local state from Phase 1 to all Task Managers
        BroadcastStream<HashMap> broadcastFrequency = phaseOneStream
                .broadcast(rulesStateDescriptor);

        // Connect Broadcast Stream and EdgeDepr Stream to build global model
        SingleOutputStreamOperator<Tuple2<EdgeEventDepr,Integer>> phaseTwoStream = edgesWindowed
                .keyBy(new KeySelector<EdgeEventDepr, Integer>() {
                    @Override
                    public Integer getKey(EdgeEventDepr value) throws Exception {
                        return value.getEdge().getOriginVertex();
                    }
                })
                .connect(broadcastFrequency)
                .process(new MatchFunctionEdges());

        DataStream<String> sideOutputStream = phaseTwoStream.getSideOutput(outputTag);
        sideOutputStream.print();

        //Print result in human-readable way
             // Tuple3 (VertexDepr, VertexDepr, Partition) --> e.g. (4,2,0) is EdgeDepr(4,2) located in Parttition 0
 /*       phaseTwoStream.map(new MapFunction<Tuple2<EdgeEventDepr, Integer>, Tuple3<Integer, Integer, Integer>>() {
            public Tuple3<Integer, Integer, Integer> map(Tuple2<EdgeEventDepr, Integer> input) {
                return new Tuple3<>(input.f0.getEdge().getOriginVertexDepr(), input.f0.getEdge().getDestinVertexDepr(), input.f1);
            }
        }).print();*/



        // *** Job Analytics



        // ### Finally, execute the job in Flink
        env.execute();

    }
}
