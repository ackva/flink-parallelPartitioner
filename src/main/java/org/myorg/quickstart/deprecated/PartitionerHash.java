package org.myorg.quickstart.deprecated;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.myorg.quickstart.partitioners.GraphPartitionerImpl;
import org.myorg.quickstart.utils.EdgeEventGelly;
import org.myorg.quickstart.utils.GraphCreatorGelly;
import org.myorg.quickstart.partitioners.deprecatedFunctions.MatchFunctionPartitioner;
import org.myorg.quickstart.partitioners.windowFunctions.ProcessWindowGelly;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

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
 *           - hash
 */
public class PartitionerHash extends GraphPartitionerImpl {

    public static final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final static Class<Tuple2<Integer, ArrayList<Integer>>> typedTuple = (Class<Tuple2<Integer, ArrayList<Integer>>>) (Class<?>) Tuple2.class;
    final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(typedTuple,new GenericTypeInfo<>(Integer.class),new GenericTypeInfo<>(Integer.class));

    // Static variables for debugging, testing, etc.
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
        MatchFunctionPartitioner matchFunction = new MatchFunctionPartitioner(algorithm, k, lambda);
        MapStateDescriptor<String, Tuple2<Integer, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO,tupleTypeInfo);

        // Environment setup
        env.setParallelism(k);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Generate OR FileRead graph -- as from arguments
        GraphCreatorGelly edgeGraph = getGraph(graphSource, graphSize);
        DataStream<EdgeEventGelly>edgeStream = edgeGraph.getEdgeStream(env);

        DataStream<Tuple2<EdgeEventGelly,Integer>> taggedEdges = edgeStream
                .map(new MapFunction<EdgeEventGelly, Tuple2<EdgeEventGelly, Integer>>() {
                    @Override
                    public Tuple2<EdgeEventGelly, Integer> map(EdgeEventGelly value) throws Exception {
                        Random rand = new Random();

                        return new Tuple2<>(value,rand.nextInt(k));
                    }
                });

        // Final Step -- Custom Partition, based on pre-calculated ID
        DataStream partitionedEdges = taggedEdges.partitionCustom(new PartitionByTag(),1);

        //Print result in human-readable way --> e.g. (4,2,0) means: EdgeDepr(4,2) partitioned to machineId 0

        partitionedEdges.map(new MapFunction<Tuple2<EdgeEventGelly, Integer>, Tuple3<Integer, Integer, Integer>>() {
            public Tuple3<Integer, Integer, Integer> map(Tuple2<EdgeEventGelly, Integer> input) {
                return new Tuple3<>(Integer.parseInt(input.f0.getEdge().f0.toString()), Integer.parseInt(input.f0.getEdge().f1.toString()), input.f1);
            }
        }).writeAsText(outputPathPartitions.replaceAll(":","_"));

        /*
        stateStream.
                filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return  ! value.equals("ignore");
                    }
                }).writeAsText(outputPathLogging.replaceAll(":","_"));
*/

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
