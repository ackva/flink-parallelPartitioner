package org.myorg.quickstart.deprecated.old;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.OutputTag;
import org.myorg.quickstart.deprecated.EdgeEventDepr;
import org.myorg.quickstart.deprecated.EdgeSimple;
import org.myorg.quickstart.deprecated.GraphCreator;

import java.util.ArrayList;
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
        List<EdgeEventDepr> edgeEventDeprs = getGraph(graphSize); // see below

        // ### Create EdgeDepr Stream from input graph
        // Assign timestamps to the stream
        KeyedStream keyedEdgeStream = env.fromCollection(edgeEventDeprs)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<EdgeEventDepr>() {
                    @Override
                    public long extractAscendingTimestamp(EdgeEventDepr element) {
                        return element.getEventTime();
                    }
                })
                .keyBy(new KeySelector<EdgeEventDepr, Integer>() {
                    @Override
                    public Integer getKey(EdgeEventDepr value) throws Exception {
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
        //windowedEdgeStream.printPhaseOne();
        //sideOutputStream.printPhaseOne();

        KeySelector abc = new KeySelector<Tuple2<DataStream<EdgeEventDepr>,Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple2<DataStream<EdgeEventDepr>,Integer> value) throws Exception {
                return value.f1;
            }
        };


        //DataStream<Tuple2<DataStream<EdgeEventDepr>,Integer>> testUtilsStream = DataStreamUtils.collect(windowedEdgeStream);
               //.reinterpretAsKeyedStream(windowedEdgeStream, abc);

        //testUtilsStream

        List<Tuple2<DataStream<EdgeEventDepr>,Integer>> allSubStreams = new ArrayList<>();

/*
        Iterator<Tuple2<DataStream<EdgeEventDepr>,Integer>> myOutput = DataStreamUtils.collect(testUtilsStream);

        while (myOutput.hasNext())
            allSubStreams.add(myOutput.next());

        int counterBroadcast=0;
        for (Tuple2 t : allSubStreams) {
            counterBroadcast++;
            System.out.println(counterBroadcast + " test --- " + t);
        }
*/


        ThreadTesting T1 = new ThreadTesting( "Thread-1");
        T1.start();

        // ### Finally, execute the job in Flink
        env.execute();
    }

    public static List<EdgeEventDepr> getGraph(int graphSize) {
        System.out.println("Number of edges: " + graphSize);
        GraphCreator tgraph = new GraphCreator();
        tgraph.generateGraphOneTwoToAny(graphSize);
        List<EdgeSimple> edgeList = tgraph.getEdges();
        // Assign event time (=now) for every edge and printPhaseOne this list
        List<EdgeEventDepr> edgeEventDeprs = new ArrayList<>();
        for (int i = 0; i < graphSize; i++)
            edgeEventDeprs.add(new EdgeEventDepr(edgeList.get(i)));

        return edgeEventDeprs;
    }
}

