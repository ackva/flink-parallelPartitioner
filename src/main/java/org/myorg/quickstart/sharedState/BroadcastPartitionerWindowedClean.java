package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Value state testing (of of "managed keyed states" in Flink)
 *
 */

public class BroadcastPartitionerWindowedClean {

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {

        // Argument fetching
        int graphSize = 100;

        // Environment setup
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // ### Generate graph and make "fake events" (for window processing)
        // Generate a graph
        System.out.println("Number of edges: " + graphSize);
        TestingGraph tgraph = new TestingGraph();
        tgraph.generateGraphOneTwoToAny(graphSize);
        List<EdgeSimple> edgeList = tgraph.getEdges();
        // Assign event time (=now) for every edge and print this list
        List<EdgeEvent> edgeEvents = new ArrayList<>();
        for (int i = 0; i < graphSize; i++)
            edgeEvents.add(new EdgeEvent(edgeList.get(i)));

        // ### Create Edge Stream from input graph
        // Assign timestamps to the stream
        KeyedStream keyedEdgeStream = env.fromCollection(edgeEvents)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<EdgeEvent>() {
                    @Override
                    public long extractAscendingTimestamp(EdgeEvent element) {
                        return element.getEventTime();
                    }
                })
                .keyBy(new KeySelector<EdgeEvent, Integer>() {
                    @Override
                    public Integer getKey(EdgeEvent value) throws Exception {
                        return value.getEdge().getOriginVertex();
                    }
                });

        SingleOutputStreamOperator windowedEdgeStream = keyedEdgeStream
                .timeWindow(Time.milliseconds(1))
                .trigger(CountTrigger.of(5))// --> used to show that behavior within same window is similar
                .process(new ProcessEdgeWindowWithSideOutput() {
                    //.process(new ProcessEdgeWindow(broadcastRulesStream, env) {
                });

        final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

        DataStream<String> sideOutputStream = windowedEdgeStream.getSideOutput(outputTag);
        windowedEdgeStream.print();
        sideOutputStream.print();

        // ### Finally, execute the job in Flink
        env.execute();
    }
}
