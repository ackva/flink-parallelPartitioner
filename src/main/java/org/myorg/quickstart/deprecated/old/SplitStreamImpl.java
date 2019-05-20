package org.myorg.quickstart.deprecated.old;

        import org.apache.flink.streaming.api.TimeCharacteristic;
        import org.apache.flink.streaming.api.collector.selector.OutputSelector;
        import org.apache.flink.streaming.api.datastream.*;
        import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
        import org.myorg.quickstart.deprecated.EdgeSimple;
        import org.myorg.quickstart.deprecated.GraphCreator;

        import java.util.*;

public class SplitStreamImpl {

    public static void main(String[] args) throws Exception {

        // Argument fetching
        int graphSize = 100;

        // Environment setup
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // ### Generate graph and make "fake events" (for window processing)
        // Generate a graph
        System.out.println("Number of edges: " + graphSize);
        GraphCreator tgraph = new GraphCreator();
        tgraph.generateGraphOneTwoToAny(graphSize);
        List<EdgeSimple> edgeList = tgraph.getEdges();
        // Assign event time (=now) for every edge and printPhaseOne this list

        ArrayList<Integer> testListIntegers = new ArrayList<>();
        for (int i = 0; i < graphSize; i++)
            testListIntegers.add(i);

        DataStream<Integer> integerStream = env.fromCollection(testListIntegers);

        SplitStream<Integer> split = integerStream.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                List<String> output = new ArrayList<String>();
                if (value % 2 == 0) {
                    output.add("even");
                }
                else {
                    output.add("odd");
                }
                return output;
            }
        });

        DataStream<Integer> even = split.select("even");
        DataStream<Integer> odd = split.select("odd");
        DataStream<Integer> all = split.select("even","odd");

        even.print();

        // ### Finally, execute the job in Flink*/
        env.execute();

    } // close main method


}
