package org.myorg.quickstart.sharedState;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashMap;
import java.util.List;

public class PhasePartitioner {

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {

        // Argument fetching
        int graphSize = 30;
        int windowSizeInMs = 500;

        // Environment setup
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Generate a new synthetic of edges
        TestingGraph edgeGraph = new TestingGraph("two",graphSize);
        DataStream<EdgeEvent> edgeSteam = edgeGraph.getEdgeStream(env);

        DataStream phaseOneStream = edgeSteam
                .keyBy(new KeySelector<EdgeEvent, Integer>() {
                    @Override
                    public Integer getKey(EdgeEvent value) throws Exception {
                        return value.getEdge().getOriginVertex();
                    }
                })
                .timeWindow(Time.milliseconds(windowSizeInMs))
                .process(new ProcessFirstPhase() {
                });
        phaseOneStream.print();
        
        // ### Finally, execute the job in Flink
        env.execute();

    }
}
