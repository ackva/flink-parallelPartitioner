package org.myorg.quickstart.deprecated;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @Arguments:
 *      1) graphSource:
 *           "1": generated on Runtime
 *           "2": from file
 *      2) input path
 *           - will be ignored, if GraphSource == 1
 *      3) output type
 *           - "1": printed on screen
 *           - "2": written to file
 *      4) output path
 *           - will be ignored, if output type == 1
 *      5)  Algorithm (Optional)
 *           - hrdf - default lambda
 *           - greedy
 *           - hash
 */
public class PhasePartitioner {

    public static final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final static Class<Tuple2<Integer, ArrayList<Integer>>> typedTuple = (Class<Tuple2<Integer, ArrayList<Integer>>>) (Class<?>) Tuple2.class;
    final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(typedTuple,new GenericTypeInfo<>(Integer.class),new GenericTypeInfo<>(Integer.class));

    // Static variables for debugging, testing, etc.
    public static long windowSizeInMs = 2000;
    public static long sleep = windowSizeInMs/100;

    public static void main(String[] args) throws Exception {

/*
        if (!parseParameters(args)) {
            return;
        }
*/

        // Argument fetching
        int graphSize = 20;

        // Environment setup
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // MapState Descriptor (as from data artisans)
        MapStateDescriptor<String, Tuple2<Integer, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO,tupleTypeInfo);

        // Generate a new synthetic graph of edges
        GraphCreator edgeGraph = new GraphCreator("byOrigin",graphSize);
        edgeGraph.printGraph();

        // Create initial EdgeStream
        DataStream<EdgeEvent> edgeSteam = edgeGraph.getEdgeStream(env);

        // *** PHASE 1 ***
        // Process edges to build the local model
        DataStream<HashMap> phaseOneStream = edgeSteam
                .keyBy(new KeySelector<EdgeEvent, Integer>() {
                    @Override
                    public Integer getKey(EdgeEvent value) throws Exception {
                        return value.getEdge().getOriginVertex();
                    }
                })
                .timeWindow(Time.milliseconds(windowSizeInMs))
                .process(new ProcessFirstPhase() {
                });
        //phaseOneStream.print();

        // Process edges in the similar time windows to "wait" for phase 2
        DataStream<EdgeEvent> edgesWindowed = edgeSteam
                .keyBy(new KeySelector<EdgeEvent, Integer>() {
                    @Override
                    public Integer getKey(EdgeEvent value) throws Exception {
                        return value.getEdge().getOriginVertex();
                    }
                })
                .timeWindow(Time.milliseconds(windowSizeInMs))
                .process(new ProcessWindow() {
                });

        // *** Phase 2 ***
        // Broadcast local state from Phase 1 to all Task Managers
        BroadcastStream<HashMap> broadcastFrequency = phaseOneStream
                .broadcast(rulesStateDescriptor);

        // Connect Broadcast Stream and Edge Stream to build global model
        SingleOutputStreamOperator<Tuple2<EdgeEvent,Integer>> phaseTwoStream = edgesWindowed
                .keyBy(new KeySelector<EdgeEvent, Integer>() {
                    @Override
                    public Integer getKey(EdgeEvent value) throws Exception {
                        return value.getEdge().getOriginVertex();
                    }
                })
                .connect(broadcastFrequency)
                .process(new MatchFunctionEdges());

        DataStream<String> sideOutputStream = phaseTwoStream.getSideOutput(outputTag);
        sideOutputStream.print();

        //Print result in human-readable way
             // Tuple3 (Vertex, Vertex, Partition) --> e.g. (4,2,0) is Edge(4,2) located in Parttition 0
 /*       phaseTwoStream.map(new MapFunction<Tuple2<EdgeEvent, Integer>, Tuple3<Integer, Integer, Integer>>() {
            public Tuple3<Integer, Integer, Integer> map(Tuple2<EdgeEvent, Integer> input) {
                return new Tuple3<>(input.f0.getEdge().getOriginVertex(), input.f0.getEdge().getDestinVertex(), input.f1);
            }
        }).print();*/



        // *** Job Analytics



        // ### Finally, execute the job in Flink
        env.execute();

    }

    private static int graphType = 0;
    private static String inputPath = null;
    private static int outputType = 0;
    private static String outputPath = null;
    private static String algorithm = null;

    // for later
    //private static double lamda = 0.0;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if (args.length != 5) {
                System.err.println("Usage: hdrf <input edges path> <output path> <log> <partitions> <lamda> ");
                return false;
            }

            graphType = Integer.valueOf(args[0]);
            inputPath = args[1];
            outputType = Integer.valueOf(args[2]);
            outputPath = args[3];
            algorithm = args[4];
            //k = (int) Long.parseLong(args[3]);
            //lamda = Double.parseDouble(args[4]);
        } else {
            System.out.println("Executing example with default parameters and built-in default data.");
            System.out.println("Provide parameters to read input data from files.");
            System.out.println(" --> Usage: PhasePartitioner <graphType> <inputPath> <outputType> <outputPath> <algorithm>");
        }
        return true;
    }


}
