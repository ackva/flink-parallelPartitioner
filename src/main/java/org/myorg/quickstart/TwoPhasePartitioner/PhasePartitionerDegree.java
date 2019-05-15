package org.myorg.quickstart.TwoPhasePartitioner;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.myorg.quickstart.applications.ExactTriangleCount;
import org.myorg.quickstart.applications.SimpleEdgeStream;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.time.LocalDate.now;

/**
 *
 * @Arguments:
 *      1) graphSource:
 *           "0": generated on Runtime
 *           "1": from file
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
public class PhasePartitionerDegree {

    public static final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final static Class<Tuple2<Integer, ArrayList<Integer>>> typedTuple = (Class<Tuple2<Integer, ArrayList<Integer>>>) (Class<?>) Tuple2.class;
    final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(typedTuple,new GenericTypeInfo<>(Integer.class),new GenericTypeInfo<>(Integer.class));

    // Static variables for debugging, testing, etc.
    public static boolean printPhaseOne = false;
    public static boolean printPhaseTwo = false;
    public static long windowSizeInMs = 1000;
    public static long sleep = 0; //public static long sleep = windowSizeInMs/100;
    // arguments
    private static int graphType = 0;
    private static String inputPath = null;
    private static String graphSource = null;
    private static String algorithm = "";
    private static int outputType = 0;
    private static String outputPath = null;
    public static int k = 2; // parallelism - partitions
    public static double lambda = 1.0;

    public static void main(String[] args) throws Exception {

        graphSource = args[0]; // 0 = synthetic || 1 = from File
        inputPath = args[1];
        algorithm = args[2];
        String outputPathPartitions = "flinkJobOutput/job_" + new SimpleDateFormat("MM_dd-HH_mm_ss").format(new Date()) + "_" + algorithm + "_p" + k + "_s" + graphSource;
        String outputPathLogging = "flinkJobOutput/job_" + new SimpleDateFormat("MM_dd-HH_mm_ss").format(new Date()) + "_Logging";

        int graphSize = Integer.parseInt(args[3]);
        ProcessWindowGelly firstPhaseProcessor = new ProcessWindowGelly();
        MatchFunctionPartitioner matchFunction = new MatchFunctionPartitioner(algorithm);
        MapStateDescriptor<String, Tuple2<Integer, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO,tupleTypeInfo);

        // Environment setup
        env.setParallelism(k);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Generate OR FileRead graph -- as from arguments
        GraphCreatorGelly edgeGraph = getGraph(graphSource, graphSize);
        DataStream<EdgeEventGelly>edgeStream = edgeGraph.getEdgeStream(env);

        // *** PHASE 1 ***
        //Process edges to build the local model for broadcast
        DataStream<HashMap<Long, Long>> phaseOneStream = edgeStream
                .keyBy(new KeySelector<EdgeEventGelly, Long>() {
                    @Override
                    public Long getKey(EdgeEventGelly value) throws Exception {
                        return Long.parseLong(value.getEdge().f0.toString());
                    }
                })
                .timeWindow(Time.milliseconds(windowSizeInMs))
                .process(new ProcessWindowDegree());
        //phaseOneStream.print();

        // Process edges in the similar time windows to "wait" for phase 2
        DataStream<EdgeEventGelly> edgesWindowed = edgeStream
                .keyBy(new KeySelector<EdgeEventGelly, Integer>() {
                    @Override
                    public Integer getKey(EdgeEventGelly value) throws Exception {
                        return Integer.parseInt(value.getEdge().f0.toString());
                    }
                })
                .timeWindow(Time.milliseconds(windowSizeInMs))
                .process(firstPhaseProcessor);

        // *** Phase 2 ***
        // Broadcast local state from Phase 1 to all Task Managers
        BroadcastStream<HashMap<Long, Long>> broadcastStateStream = phaseOneStream
                .broadcast(rulesStateDescriptor);

        // Connect Broadcast Stream and Edge Stream to build global model
        SingleOutputStreamOperator<Tuple2<EdgeEventGelly,Integer>> phaseTwoStream = edgesWindowed
                .keyBy(new KeySelector<EdgeEventGelly, Integer>() {
                    @Override
                    public Integer getKey(EdgeEventGelly value) throws Exception {
                        return Integer.parseInt(value.getEdge().f0.toString());
                    }
                })
                .connect(broadcastStateStream)
                .process(matchFunction);

        // Final Step -- Custom Partition, based on pre-calculated ID
        DataStream partitionedEdges = phaseTwoStream.partitionCustom(new PartitionByTag(),1);

        //Print result in human-readable way --> e.g. (4,2,0) means: Edge(4,2) partitioned to machineId 0

        partitionedEdges.map(new MapFunction<Tuple2<EdgeEventGelly, Integer>, Tuple3<Integer, Integer, Integer>>() {
            public Tuple3<Integer, Integer, Integer> map(Tuple2<EdgeEventGelly, Integer> input) {
                return new Tuple3<>(Integer.parseInt(input.f0.getEdge().f0.toString()), Integer.parseInt(input.f0.getEdge().f1.toString()), input.f1);
            }
        }).writeAsText(outputPathPartitions.replaceAll(":","_"));

        DataStream<String> stateStream = phaseTwoStream.getSideOutput(outputTag)
                .keyBy(new KeySelector<String, Integer>() {
                 @Override
                public Integer getKey(String value) throws Exception {
                    return 1;
                 }
                })
                .reduce(new ReduceFunction<String>() {

                    int iteration = 0;
                    int longestString = 0;
                    @Override
                    public String reduce(String value1, String value2) throws Exception {
                        ++iteration;
                        //System.out.println("Value 1: " + value1.length() + " -- Value 2: " + value2.length());
                        if (value2.length()>= longestString && iteration > 70) {
                            longestString = value2.length();
                            return iteration + " -- " + value2;
                        } else {
                            return "ignore";
                        }

                    }
                });

        stateStream.
                filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return  ! value.equals("ignore");
                    }
                }).writeAsText(outputPathLogging.replaceAll(":","_"));


        // ### Execute the job in Flink
        //System.out.println(env.getExecutionPlan());
        JobExecutionResult result = env.execute(createJobName(algorithm,k, graphSource));

        System.out.println("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute"+"\n");//appends the string to the file
        System.out.println("The job took " + result.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute"+"\n");

    }

    private static GraphCreatorGelly getGraph(String generateGraph, int graphSize) throws Exception {
        GraphCreatorGelly edgeGraph;
        if (generateGraph.equals("0")) {
            // GENERATE GRAPH
            edgeGraph = new GraphCreatorGelly("two",graphSize, env);
            edgeGraph.printGraph();
        } else if (generateGraph.equals("1")) {
            // READ GRAPH FROM FILE
            edgeGraph = new GraphCreatorGelly("file", inputPath, env);
        } else throw new Exception("check the input for generate graph");

        return edgeGraph;
    }

    private static String createJobName(String algorithm, int k, String generateGraph) {
        String jobName = "Flink Job Name not determined";
        if (generateGraph.equals("0")) {
            jobName = "Runtime-generated Graph";
        } else if (generateGraph.equals("1")) {
            // READ GRAPH FROM FILE
            jobName = "File-Read Graph";
        } else {
            jobName = "Undefined Graph";
        }

        return jobName + " Partitioning with " + algorithm + ". parallelism " + k;
    }

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

    public static class PartitionByTag implements Partitioner<Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            return key % numPartitions;
        }
    }

}
