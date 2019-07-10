/*
package org.myorg.quickstart.batchApplication;

        import org.apache.flink.api.common.functions.FilterFunction;
        import org.apache.flink.api.common.functions.MapFunction;
        import org.apache.flink.api.java.utils.ParameterTool;
        import org.apache.flink.graph.Edge;
        import org.apache.flink.streaming.api.datastream.DataStream;
        import org.apache.flink.streaming.api.datastream.IterativeStream;
        import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
        import org.apache.flink.types.NullValue;
        import org.myorg.quickstart.applications.GraphStream;
        import org.myorg.quickstart.applications.SimpleEdgeStream;
        import org.myorg.quickstart.partitioners.WinBroIntegratable;
        import org.myorg.quickstart.utils.ConnectedComponents;
        import org.myorg.quickstart.utils.DisjointSet;
        import org.myorg.quickstart.utils.DumSink4;

public class IterativePageRankMockup {


    private static String InputPath = null;


    public static void main(String[] args) throws Exception {

        InputPath = args[0];

        // Checking input parameters
        //final ParameterTool params = ParameterTool.fromArgs(args);

        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Make parameters available in Web Interface
        //env.getConfig().setGlobalJobParameters(params);

        env.setParallelism(2);

        //DataStream<Edge<Integer, NullValue>> edgeStream;

            DataStream<Edge<Integer, NullValue>> edgeStream;
            edgeStream = new WinBroIntegratable(env, InputPath, "hdrf", 0, 2, 2, 1, 0)
                    .partitionGraph();
            GraphStream<Integer, NullValue, NullValue> graph = new SimpleEdgeStream<Integer, NullValue>(edgeStream, env);

            */
/*DataStream<DisjointSet<Integer>> cc1 = graph.aggregate(new ConnectedComponents<Integer, NullValue>(5000, outputPath));
            cc1.addSink(new DumSink4());
*//*


        IterativeStream<Edge<Integer, NullValue>> iteration = edgeStream.iterate();

//        iteration.print();

        DataStream<Long> minusOne = iteration.map(new MapFunction<Edge<Integer, NullValue>, Edge<Integer, NullValue>>() {
            @Override
            public Edge<Integer, NullValue> map(Edge<Integer, NullValue> value) throws Exception {
                return value ;
            }
        });


        //minusOne.print();

*/
/*        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value > 0);
            }
        });

        iteration.closeWith(stillGreaterThanZero);

        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value <= 0);
            }
        });

        lessThanZero.print();*//*


        env.execute();

    }
}


*/
