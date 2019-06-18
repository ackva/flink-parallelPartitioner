package org.myorg.quickstart.testsigma;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.functions.ProcessFunction.OnTimerContext;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.applications.SimpleEdgeStream;
import org.myorg.quickstart.utils.EdgeEventGelly;
import org.myorg.quickstart.utils.GraphCreatorGelly;

import javax.annotation.Nullable;
import java.util.ArrayList;

public class TestTimedStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CountWithTimeoutFunction cwtf = new CountWithTimeoutFunction();
        // the source data stream
        //SimpleEdgeStream<Integer, NullValue> edges = getGraphStream(env);
        ArrayList<Tuple2<String, String>> input = new ArrayList<Tuple2<String, String>>();
        for (int j = 0; j < 200000; j++) {
            for (int i = 0; i < 5; i++) {
                String f0 = String.valueOf(1);
                String f1 = String.valueOf("deineMutter");
                input.add(new Tuple2<String, String>(f0,f1));
            }
        }


        DataStream<Tuple2<String, String>> stream = env.fromCollection(input)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, String>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<String, String> element) {
                        return System.currentTimeMillis();
                    }
                });


        // apply the process function onto a keyed stream
        DataStream<Tuple2<String, Long>> result = stream
                .keyBy(0)
                .process(cwtf);

        result.print();

        JobExecutionResult jobRes = env.execute("Streaming Items and Partitioning");

    }


}
