/*
package org.myorg.quickstart.sharedState;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.myorg.quickstart.deprecated.old.EdgeDepr;

import java.util.ArrayList;
import java.util.List;

public class QuickTests {


        public static void main(String[] args) throws Exception {

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);

            // Generate "random" edges as input for stream
            List<Integer> keyedInput = getIntegerGraph();

            final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

            DataStream<Integer> input = env.fromCollection(keyedInput);
            SingleOutputStreamOperator<Integer> mainDataStream = input
                    .process(new ProcessFunction<Integer, Integer>() {

                        @Override
                        public void processElement(
                                Integer value,
                                Context ctx,
                                Collector<Integer> out) throws Exception {
                            // emit data to regular output
                            out.collect(value);

                            // emit data to side output
                            ctx.output(outputTag, "sideout-" + String.valueOf(value));
                        }
                    });
            DataStream<String> sideOutputStream = mainDataStream.getSideOutput(outputTag);
            sideOutputStream.printPhaseOne();
            env.execute();
        }

        // Get the graph to stream
    public static List<EdgeDepr> getGraph() {
        List<EdgeDepr> keyedInput = new ArrayList<>();
        keyedInput.add(new EdgeDepr(new VertexDepr(1), new VertexDepr(2)));
        keyedInput.add(new EdgeDepr(new VertexDepr(1), new VertexDepr(3)));
        keyedInput.add(new EdgeDepr(new VertexDepr(1), new VertexDepr(4)));
        keyedInput.add(new EdgeDepr(new VertexDepr(1), new VertexDepr(5)));
        keyedInput.add(new EdgeDepr(new VertexDepr(2), new VertexDepr(3)));
        keyedInput.add(new EdgeDepr(new VertexDepr(2), new VertexDepr(4)));
        keyedInput.add(new EdgeDepr(new VertexDepr(2), new VertexDepr(5)));
        keyedInput.add(new EdgeDepr(new VertexDepr(2), new VertexDepr(6)));
        keyedInput.add(new EdgeDepr(new VertexDepr(3), new VertexDepr(4)));
        keyedInput.add(new EdgeDepr(new VertexDepr(3), new VertexDepr(5)));
        keyedInput.add(new EdgeDepr(new VertexDepr(3), new VertexDepr(6)));
        keyedInput.add(new EdgeDepr(new VertexDepr(3), new VertexDepr(7)));
        keyedInput.add(new EdgeDepr(new VertexDepr(4), new VertexDepr(5)));
        keyedInput.add(new EdgeDepr(new VertexDepr(4), new VertexDepr(6)));
        keyedInput.add(new EdgeDepr(new VertexDepr(4), new VertexDepr(7)));
        keyedInput.add(new EdgeDepr(new VertexDepr(4), new VertexDepr(8)));

        return keyedInput;
    }

    // Get the graph to stream
    public static List<Integer> getIntegerGraph() {
        List<Integer> keyedInput = new ArrayList<>();
        for (int i = 0; i < 10; i++)
            keyedInput.add(i);
        return keyedInput;
    }

    // Get the graph to stream
    public static List<Tuple2<Integer, Integer>> getPseudoGraph() {
        List<Tuple2<Integer, Integer>> graph = new ArrayList<>();
        Tuple2<Integer, Integer> tuple2 = new Tuple2<>(-1,-1);
        int temp = 0;
        for (int i = 0; i < 10; i++) {
            temp = i;
            tuple2.f0 = temp;
            tuple2.f1 = temp + 1;
            graph.add(tuple2);
        }
        return graph;
    }

    // Get the graph to stream
    public static List<String> getStringGraph() {
        List<String> keyedInput = new ArrayList<>();
        for (int i = 0; i < 10; i++)
            keyedInput.add("String " + i);
        return keyedInput;
    }

}
*/
