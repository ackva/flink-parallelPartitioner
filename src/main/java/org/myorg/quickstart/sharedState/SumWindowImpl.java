package org.myorg.quickstart.sharedState;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.*;

/**
 *
 *  WINDOW SIZE BY *** COUNT **** -> 5 stream items = 1 window
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

public class SumWindowImpl {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);

        // Generate a list of tuples with field ID and field CarCount Tuple2<SENSOR; CARS>
        List<Tuple2<Integer, Integer>> vehicleCountPerSensor = new ArrayList<>();
        Random rand = new Random();
        for (int i = 0; i < 500; i++) {
            vehicleCountPerSensor.add(new Tuple2<>(i%5,rand.nextInt(50)));
            System.out.println(vehicleCountPerSensor.get(i).f0 + " _ " + vehicleCountPerSensor.get(i).f1);
        }

        // Create Stream from list above
        // Key by sensor ID and after 2 occurrences, emit a new "window" which sums up field 1 (car count)
        DataStream<Tuple2<Integer, Integer>> vehicleCount = env.fromCollection(vehicleCountPerSensor)
                .keyBy(0)
                .countWindow(5)
                .sum(1);

        // Print the results
        vehicleCount.print();

        env.execute();
    }
}
