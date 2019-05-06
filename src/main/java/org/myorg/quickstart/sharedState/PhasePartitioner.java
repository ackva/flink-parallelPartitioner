package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Int;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
        int graphSize = 3;
        int windowSizeInMs = 500;

        // Environment setup
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Sideoutput
        final OutputTag<List<EdgeEvent>> outputTag = new OutputTag<List<EdgeEvent>>("edgesInWindow"){};


        // MapState Descriptor (as from data artisans)
        MapStateDescriptor<String, Tuple2<Integer, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                tupleTypeInfo
        );

        // Generate a new synthetic of edges
        TestingGraph edgeGraph = new TestingGraph("byOrigin",graphSize);
        DataStream<EdgeEvent> edgeSteam = edgeGraph.getEdgeStream(env);

        SingleOutputStreamOperator<HashMap> phaseOneStream = edgeSteam
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

        DataStream<List<EdgeEvent>> sideOutputStream = phaseOneStream.getSideOutput(outputTag);

        MatchFunctionEdges matchRules = new MatchFunctionEdges();
        //MatchFunctionEdgeList matchRules2 = new MatchFunctionEdgeList();

        BroadcastStream<HashMap> broadcastFrequency = phaseOneStream
                .broadcast(rulesStateDescriptor);

        DataStream<Tuple2<EdgeEvent,Integer>> phaseTwoStream = sideOutputStream
                .flatMap(new FlatMapFunction<List<EdgeEvent>, EdgeEvent>() {
                    @Override
                    public void flatMap(List<EdgeEvent> value, Collector<EdgeEvent> out) throws Exception {
                            for (EdgeEvent e: value)
                                out.collect(e);
                    }
                })
                .keyBy(new KeySelector<EdgeEvent, Integer>() {
                    @Override
                    public Integer getKey(EdgeEvent value) throws Exception {
                        return value.getEdge().getOriginVertex();
                    }
                })
                .connect(broadcastFrequency)
                .process(matchRules);

        phaseTwoStream.print();
/*
        DataStream<Tuple2<EdgeEvent,Integer>> phaseTwoStream = phaseOneStream
                .keyBy(new KeySelector<EdgeEvent, Integer>() {
                    @Override
                    public Integer getKey(EdgeEvent value) throws Exception {
                        return value.getEdge().getOriginVertex();
                    }
                })
                .connect(broadcastFrequency)
                .process(matchRules);
*/



/*

        sideOutputStream.print();

        // Print result as Tuple3 (Vertex, Vertex, Partition) --> e.g. (4,2,0) is Edge(4,2) located in Parttition 0
        phaseTwoStream.map(new MapFunction<Tuple2<EdgeEvent, Integer>, Tuple3<Integer, Integer, Integer>>() {
            public Tuple3<Integer, Integer, Integer> map(Tuple2<EdgeEvent, Integer> input) {
                return new Tuple3<>(input.f0.getEdge().getOriginVertex(), input.f0.getEdge().getDestinVertex(), input.f1);
            }
        }).print();
*/

        // ### Finally, execute the job in Flink
        env.execute();

    }
}
