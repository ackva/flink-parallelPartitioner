/*
package org.myorg.quickstart.partitioners;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.library.ConnectedComponents;
import org.apache.flink.graph.streaming.summaries.DisjointSet;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.yarn.state.Graph;
import org.myorg.quickstart.jobstatistics.LoadBalanceCalculator;
import org.myorg.quickstart.jobstatistics.VertexCut;
import org.myorg.quickstart.jobstatistics.VertexCutImpl;
import org.myorg.quickstart.partitioners.windowFunctions.ProcessWindowGelly;
import org.myorg.quickstart.utils.CustomKeySelector5;
import org.myorg.quickstart.utils.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.time.LocalDate.now;

*/
/**
 *
 *
 *
 * @Arguments:
 *      0) graphSource:
 *           "0": generated on Runtime
 *           "1": from file
 *      1) input path
 *           - will be ignored, if GraphSource == 0
 *      2)  Algorithm (Optional)
 *           - hrdf - default lambda
 *           - hash
 *      3) Graph size
 *          - will be ignored if graphsource == 1
 *      4) Parallelism in "global model" step --> parallel or non-parallel HDRF
 *          - 1 (default)
 *          - X whatever
 *      5) General parallelism
 *          - must be > 1
 *      6) "graph name" --> used for logging, e.g. "twitter"
 *
 *   Example (local testing in IntelliJ:
 *   1 C:\flinkJobs\input\streamInput199.txt dbh 100 2 2 streamInput
 *
 *//*

public class GraphPartitioner {

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
    private static int globalPhase = 1;
    private static int outputType = 0;
    private static String outputPath = null;
    public static int k = 2; // parallelism - partitions
    public static double lambda = 1.0;

    public static void main(String[] args) throws Exception {

        graphSource = args[0]; // 0 = synthetic || 1 = from File
        inputPath = args[1];
        algorithm = args[2];
        int graphSize = Integer.parseInt(args[3]);
        globalPhase = Integer.valueOf(args[4]);
        k = Integer.valueOf(args[5]);
        String graphName = args[6];
        String outputPathStatistics = args[7];
        String testing = args[8];
        boolean localRun = false;
        if (testing.equals("local")) {
            localRun = true;
        }
        String timestamp = new SimpleDateFormat("yy-MM-dd_HH-mm-ss").format(new Date());
        String outputPathPartitions = "flinkJobOutput/job_" + timestamp + "_" + algorithm + "_p" + k + "_" + graphName;
        String outputPathLogging = "flinkJobOutput/job_" + timestamp + "_Logging";

        ProcessWindowGelly firstPhaseProcessor = new ProcessWindowGelly();
        MatchFunctionPartitioner matchFunction = new MatchFunctionPartitioner(algorithm, k, lambda);
        MapStateDescriptor<String, Tuple2<Integer, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO,tupleTypeInfo);

        //System.out.println(new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()) + " timestamp for whatever you want");

        // Environment setup
        env.setParallelism(k);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // Generate OR FileRead graph -- as from arguments
        GraphCreatorGelly edgeGraph = getGraph(graphSource, graphSize);

        DataStream<EdgeEventGelly> edgeStream = edgeGraph.getEdgeStream(env);

        DataStream<EdgeEventGelly> partitionedEdges = null;

        if (algorithm.equals("hdrf") || algorithm.equals("dbh")) {
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

            // Connect Broadcast Stream and EdgeDepr Stream to build global model
            SingleOutputStreamOperator<Tuple2<EdgeEventGelly, Integer>> phaseTwoStream = edgesWindowed
                    .keyBy(new KeySelector<EdgeEventGelly, Integer>() {
                        @Override
                        public Integer getKey(EdgeEventGelly value) throws Exception {
                            return Integer.parseInt(value.getEdge().f0.toString());
                        }
                    })
                    .connect(broadcastStateStream)
                    .process(matchFunction).setParallelism(globalPhase);
            //phaseTwoStream.print();

            // Final Step -- Custom Partition, based on pre-calculated ID
            partitionedEdges = phaseTwoStream
                    .partitionCustom(new PartitionByTag(), 1)
                    .map(new MapFunction<Tuple2<EdgeEventGelly, Integer>, EdgeEventGelly>() {
                        public EdgeEventGelly map(Tuple2<EdgeEventGelly, Integer> input) {
                            return input.f0;
                        }});
        } else if (algorithm.equals("hash")) {
            partitionedEdges = edgeStream
                    .partitionCustom(new HashPartitioner<>(k),new CustomKeySelector5<>(0));
        } else {
            throw new Exception("WRONG ALGO!!");
        }

*/
/*        GraphStream<Long, NullValue, NullValue> edges = partitionedEdges.map(new MapFunction<EdgeEventGelly, DisjointSet<Long>>() {
            @Override
            public  map(EdgeEventGelly value) throws Exception {
                return null;
            }
            })*//*





        //Print result in human-readable way --> e.g. (4,2,0) means: EdgeDepr(4,2) partitioned to machineId 0
        partitionedEdges.writeAsText(outputPathPartitions.replaceAll(":","_"));


        // Attempt to lower the amount of "state" prints -- ignore for now
        */
/*DataStream<String> stateStream = phaseTwoStream.getSideOutput(outputTag)
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
                });*//*


        */
/*
        stateStream.
                filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return  ! value.equals("ignore");
                    }
                }).writeAsText(outputPathLogging.replaceAll(":","_"));
*//*


        // ### Execute the job in Flink
        System.out.println(env.getExecutionPlan());
        JobExecutionResult result = env.execute(createJobName(algorithm,k, graphSource, graphName));

        System.out.println("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute"+"\n");//appends the string to the file
        System.out.println("The job took " + result.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute"+"\n");

        // Gather statistics for the job
        String statistics = graphName + "," + algorithm + "," + timestamp +  "," + result.getNetRuntime(TimeUnit.MILLISECONDS) + ","
                + result.getNetRuntime((TimeUnit.SECONDS)) + "," + k + "," + globalPhase + "," + inputPath + ",";
        if (localRun) {
            File directory = new File("C:\\Users\\adac0\\IdeaProjects\\flink-parallelPartitioner\\" + outputPathPartitions);
            File[] partitions = directory.listFiles();
            List<File> fileList = new ArrayList<>();
            int parallelism = 0;
            for (File f : partitions) {
                fileList.add(f);
                System.out.println("File: " + f.getName());
                parallelism = +1;
            }
            double replicationFactor = new VertexCut(parallelism).calculateVertexCut(fileList);
            double load = new LoadBalanceCalculator().calculateLoad(fileList);
            statistics = statistics + replicationFactor + "," + load;

        }
        System.out.println("statistics: " + statistics);
        // graphName,algorithm,timestamp,durationInMs,durationInSec,partitions,parallelismModel,inputPath,replicationFactor,load

        try {
            FileWriter fw = new FileWriter(outputPathStatistics, true); //the true will append the new data
            fw.write(statistics + "\n");//appends the string to the file
            //fw.write("The job took " + result.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute"+"\n");
            fw.close();
        } catch (IOException ioe) {
            System.err.println("IOException: " + ioe.getMessage());
        }


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

    private static String createJobName(String algorithm, int k, String generateGraph, String graphName) {
        String jobName = "Flink Job Name not determined";
        if (generateGraph.equals("0")) {
            jobName = "Runtime-generated Graph";
        } else if (generateGraph.equals("1")) {
            // READ GRAPH FROM FILE
            jobName = graphName + " Graph";
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

*/
/*            graphType = Integer.valueOf(args[0]);
            inputPath = args[1];
            outputType = Integer.valueOf(args[2]);
            outputPath = args[3];
            algorithm = args[4];
            globalPhase = Integer.valueOf(args[5]);
            //k = (int) Long.parseLong(args[3]);
            //lamda = Double.parseDouble(args[4]);*//*

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

*/
