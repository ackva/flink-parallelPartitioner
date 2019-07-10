package org.myorg.quickstart.applications;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.partitioners.WinBroIntegratable;
import org.myorg.quickstart.utils.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by zainababbas on 07/02/2017.
 */
public class TestingIterative {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<String> stringInput = new ArrayList<>();
        stringInput.add("A");
        stringInput.add("B");
        stringInput.add("C");
        stringInput.add("D");

        //DataStream<Edge<Integer, NullValue>> edgeStream;

        DataStream<String> edgeStream = env.fromCollection(stringInput);

        IterativeStream<String> abc = edgeStream
                .iterate(5000);

        abc.print();

        JobExecutionResult result1 = env.execute("Connected Components Streaming ");

    }
}