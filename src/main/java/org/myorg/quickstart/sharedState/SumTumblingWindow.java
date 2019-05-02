package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.myorg.quickstart.sharedState.EdgeEvent;
import org.myorg.quickstart.sharedState.EdgeSimple;
import org.myorg.quickstart.sharedState.TestingGraph;

import java.util.ArrayList;
import java.util.List;

/**
 *
 *  WINDOW SIZE BY *** TIME **** -> 5 seconds = 1 window
 *
 */


/**
 * Very simple example of a stream with sensors and cars (left: "sensor ID"; right: "number of cars since last measure point")
 * Every two time points (i.e. Window size 2) of each sensor, its car count is summed up.
 *
 * 0 _ 3 --> ID 0 measured 3 cars
 * 1 _ 4 --> ID 1 measured 4 cars
 * 2 _ 1
 * 3 _ 2
 * 4 _ 0
 * 0 _ 1 --> "next fictive round"
 * 1 _ 2
 * 2 _ 2
 * 3 _ 0
 * 4 _ 4
 *
 * --> OUTPUT
 * 2> (0,4) --> sensor 0 measured 4 cars in total
 * 2> (1,6)
 * 1> (4,4)
 * 2> (2,3)
 * 2> (3,2)
 */

public class SumTumblingWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);

        int graphSize = 5000;
        // Generate a list of tuples with field ID and field CarCount Tuple2<SENSOR; CARS>
        List<EdgeSimple> edges = new ArrayList<>();
        TestingGraph tgraph = new TestingGraph();
        tgraph.generateGraphOneToAny(graphSize);
        edges = tgraph.getEdges();

        // Assign event time (=now) for every edge and print this list
        List<EdgeEvent> edgeEvents = new ArrayList<>();
        for (int i = 0; i < graphSize; i++) {
            edgeEvents.add(new EdgeEvent(edges.get(i)));
            System.out.println("Edge: "+ edgeEvents.get(i).getEdge().getOriginVertex() + " "
                    +edgeEvents.get(i).getEdge().getDestinVertex() + " -- Time: " + edgeEvents.get(i).getEventTime());
        }

        // Assign timestamps to the edges (as "fake" events)
        DataStream<EdgeEvent> sourceData = env.fromCollection(edgeEvents)
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<EdgeEvent>() {

                    @Override
                    public long extractAscendingTimestamp(EdgeEvent element) {
                        return element.getEventTime();
                    }
                });

        // Stream with Tuple2<EdgeEvent,"originVertex">
        // Sum up origin Vertex per Window (in total, it MUST add up to "graphSize" variable
        DataStream<Tuple2<EdgeEvent, Integer>> windowTestStream2 = sourceData
                .map(new MapFunction<EdgeEvent, Tuple2<EdgeEvent, Integer>>() {
                    @Override
                    public Tuple2<EdgeEvent, Integer> map(EdgeEvent value)
                            throws Exception {
                        return new Tuple2<>(value,value.getEdge().getOriginVertex());
                    }
                })
                .keyBy(1) // keyBy first Vertex ("originVertex")
                .timeWindow(Time.milliseconds(5))
                .sum(1);
        // Print the results
        windowTestStream2.print();

        env.execute();
    }


}
