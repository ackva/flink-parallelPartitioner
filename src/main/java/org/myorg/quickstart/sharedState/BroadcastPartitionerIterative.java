package org.myorg.quickstart.sharedState;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.OutputTag;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * This approach aims the following:
 * "big" datastream of edges
 *  .window
 *  .process()
 *      --> create 1 new substream of edges per window
*
 * for each substream{
 *     analyze(edge)
 *     updateRulesWithBroadcast()
 *
 * }
 *
 *
 */

public class BroadcastPartitionerIterative {

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {

        // Argument fetching
        int graphSize = 100;

        // Environment setup
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // ### Generate graph and make "fake events" (for window processing)
        List<EdgeEvent> edgeEvents = getGraph(graphSize); // see below

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
                .trigger(CountTrigger.of(5))
                .process(new ProcessEdgeWindowWithSideOutput() {
                    //.process(new ProcessEdgeWindow(broadcastRulesStream, env) {
                });

        final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

        //SingleOutputStreamOperator<Integer> mainDataStream = ...;

        DataStream<String> sideOutputStream = windowedEdgeStream.getSideOutput(outputTag);
        //windowedEdgeStream.print();
        //sideOutputStream.print();

        KeySelector abc = new KeySelector<Tuple2<DataStream<EdgeEvent>,Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple2<DataStream<EdgeEvent>,Integer> value) throws Exception {
                return value.f1;
            }
        };


        //DataStream<Tuple2<DataStream<EdgeEvent>,Integer>> testUtilsStream = DataStreamUtils.collect(windowedEdgeStream);
               //.reinterpretAsKeyedStream(windowedEdgeStream, abc);

        //testUtilsStream

        List<Tuple2<DataStream<EdgeEvent>,Integer>> allSubStreams = new ArrayList<>();

/*
        Iterator<Tuple2<DataStream<EdgeEvent>,Integer>> myOutput = DataStreamUtils.collect(testUtilsStream);

        while (myOutput.hasNext())
            allSubStreams.add(myOutput.next());

        int counter=0;
        for (Tuple2 t : allSubStreams) {
            counter++;
            System.out.println(counter + " test --- " + t);
        }
*/


        ThreadTesting T1 = new ThreadTesting( "Thread-1");
        T1.start();

        // ### Finally, execute the job in Flink
        env.execute();
    }

    public static List<EdgeEvent> getGraph(int graphSize) {
        System.out.println("Number of edges: " + graphSize);
        TestingGraph tgraph = new TestingGraph();
        tgraph.generateGraphOneTwoToAny(graphSize);
        List<EdgeSimple> edgeList = tgraph.getEdges();
        // Assign event time (=now) for every edge and print this list
        List<EdgeEvent> edgeEvents = new ArrayList<>();
        for (int i = 0; i < graphSize; i++)
            edgeEvents.add(new EdgeEvent(edgeList.get(i)));

        return  edgeEvents;
    }
}

