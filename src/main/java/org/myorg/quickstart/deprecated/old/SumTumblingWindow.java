package org.myorg.quickstart.deprecated.old;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.myorg.quickstart.deprecated.EdgeEventDepr;
import org.myorg.quickstart.deprecated.EdgeSimple;
import org.myorg.quickstart.deprecated.GraphCreator;

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
        GraphCreator tgraph = new GraphCreator();
        tgraph.generateGraphOneToAny(graphSize);
        edges = tgraph.getEdges();

        // Assign event time (=now) for every edge and printPhaseOne this list
        List<EdgeEventDepr> edgeEventDeprs = new ArrayList<>();
        for (int i = 0; i < graphSize; i++) {
            edgeEventDeprs.add(new EdgeEventDepr(edges.get(i)));
            System.out.println("EdgeDepr: "+ edgeEventDeprs.get(i).getEdge().getOriginVertex() + " "
                    + edgeEventDeprs.get(i).getEdge().getDestinVertex() + " -- Time: " + edgeEventDeprs.get(i).getEventTime());
        }

        // Assign timestamps to the edges (as "fake" events)
        DataStream<EdgeEventDepr> sourceData = env.fromCollection(edgeEventDeprs)
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<EdgeEventDepr>() {

                    @Override
                    public long extractAscendingTimestamp(EdgeEventDepr element) {
                        return element.getEventTime();
                    }
                });

        // Stream with Tuple2<EdgeEventDepr,"originVertex">
        // Sum up origin VertexDepr per Window (in total, it MUST add up to "graphSize" variable
        DataStream<Tuple2<EdgeEventDepr, Integer>> windowTestStream2 = sourceData
                .map(new MapFunction<EdgeEventDepr, Tuple2<EdgeEventDepr, Integer>>() {
                    @Override
                    public Tuple2<EdgeEventDepr, Integer> map(EdgeEventDepr value)
                            throws Exception {
                        return new Tuple2<>(value,value.getEdge().getOriginVertex());
                    }
                })
                .keyBy(1) // keyBy first VertexDepr ("originVertex")
                .timeWindow(Time.milliseconds(5))
                .sum(1);
        // Print the results
        windowTestStream2.print();

        env.execute();
    }


}
