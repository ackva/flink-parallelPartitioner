package org.myorg.quickstart.partitioners;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.myorg.quickstart.applications.SimpleEdgeStream;
import org.myorg.quickstart.jobstatistics.LoadBalanceCalculator;
import org.myorg.quickstart.jobstatistics.VertexCut;
import org.myorg.quickstart.utils.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

// ARGUMENTS: --> 1 C:\flinkJobs\input\ca-AstroPh.txt hdrf 100 2 2 streamInput flinkJobOutput\statistics\statistics.csv flinkJobOutput 1000   0    localTest
//                      input                         algo     k kModel  name              stats                        outputfolder   wind sleep  test/cluster

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
 */
public class GraphPartitionerImpl_old {

    public static final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final static Class<Tuple2<Integer, ArrayList<Integer>>> typedTuple = (Class<Tuple2<Integer, ArrayList<Integer>>>) (Class<?>) Tuple2.class;
    final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(typedTuple,new GenericTypeInfo<>(Integer.class),new GenericTypeInfo<>(Integer.class));

    // Static variables for debugging, testing, etc.
    public static long windowSizeInMs = 0;
    public static long wait = 0; //public static long sleep = windowSizeInMs/100;
    private static String inputPath = null;
    private static String printInfo = null;
    private static String algorithm = "";
    public static int keyParam = 0;
    private static int globalPhase = 0;
    private static String graphName;
    private static String outputStatistics = null;
    private static String outputPath = null;
    private static String loggingPath = null;
    private static String testing = null;
    public static int k = 2; // parallelism - partitions
    public static double lambda = 1.0;
    //public static boolean debugMode = false;
    //public static int printModulo = 20000;
    public static boolean localRun = false;

    public static void main(String[] args) throws Exception {

        parseParameters(args);

        long windowSize2 = windowSizeInMs * 2;
        String timestamp = new SimpleDateFormat("yy-MM-dd_HH-mm-ss").format(new Date());
        String folderName = "job_" + timestamp + "_" + algorithm + "_p" + k + "_" + graphName;
        String outputPathPartitions = outputPath + "/" + folderName;
        loggingPath = outputPath + "/logs_" + folderName;

        ProcessWindowGelly firstPhaseProcessor = new ProcessWindowGelly();
        MatchFunctionPartitioner matchFunction = new MatchFunctionPartitioner(algorithm, k, lambda);
        MapStateDescriptor<String, Tuple2<Integer, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO,tupleTypeInfo);

        //System.out.println(new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()) + " timestamp for whatever you want");

        // Environment setup
        env.setParallelism(k);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KeySelector<Edge<Integer, NullValue>, Integer> ks = new KeySelector<Edge<Integer, NullValue>, Integer>() {
            @Override
            public Integer getKey(Edge<Integer, NullValue> value) throws Exception {
                //System.out.println(value.f0 % 10);
                if (keyParam > 1)
                    return (value.f0 % keyParam);
                else
                    return value.f0;

            }
        };

        // Create a data stream (read from file)
        SimpleEdgeStream<Integer, NullValue> edges = getGraphStream(env);

        DataStream<Edge<Integer, NullValue>> partitionedEdges = null;

        if (algorithm.equals("hdrf") || algorithm.equals("dbh")) {
            // *** PHASE 1 ***
            //Process edges to build the local model for broadcast
            DataStream<HashMap<Integer, Integer>> phaseOneStream = edges.getEdges()
                    .keyBy(ks)
                    .timeWindow(Time.milliseconds(windowSizeInMs))
                    .process(new ProcessWindowDegree());

            // Process edges in the similar time windows to "wait" for phase 2
            DataStream<Edge<Integer, NullValue>> edgesWindowed = edges.getEdges()
                    .keyBy(ks)
                    .timeWindow(Time.milliseconds(windowSizeInMs))
                    .process(firstPhaseProcessor);

            // *** Phase 2 ***
            // Broadcast local state from Phase 1 to all Task Managers
            BroadcastStream<HashMap<Integer, Integer>> broadcastStateStream = phaseOneStream
                    .broadcast(rulesStateDescriptor);

            // Connect Broadcast Stream and EdgeDepr Stream to build global model
            SingleOutputStreamOperator<Tuple2<Edge<Integer, NullValue>, Integer>> phaseTwoStream = edgesWindowed
                    .keyBy(new KeySelector<Edge<Integer, NullValue>, Integer>() {
                        @Override
                        public Integer getKey(Edge value) throws Exception {
                            return Integer.parseInt(value.f0.toString());
                        }
                    })
                    .connect(broadcastStateStream)
                    .process(matchFunction).setParallelism(globalPhase);
            //phaseTwoStream.print();

            DataStream<String> sideOutputStream = phaseTwoStream.getSideOutput(outputTag);
            sideOutputStream.print();
            sideOutputStream.writeAsText(loggingPath.replaceAll(":","_"));

            // Final Step -- Custom Partition, based on pre-calculated ID
            partitionedEdges = phaseTwoStream
                    .partitionCustom(new PartitionByTag(), 1)
                    .map(new MapFunction<Tuple2<Edge<Integer, NullValue>, Integer>, Edge<Integer, NullValue>>() {
                        public Edge<Integer, NullValue> map(Tuple2<Edge<Integer, NullValue>, Integer> input) {
                            return input.f0;
                        }});
        } else if (algorithm.equals("hash")) {
            partitionedEdges = edges.getEdges()
                    .partitionCustom(new HashPartitioner<>(k),new CustomKeySelector5<>(0));
        } else {
            throw new Exception("WRONG ALGO!!");
        }


        //Print result in human-readable way --> e.g. (4,2,0) means: EdgeDepr(4,2) partitioned to machineId 0
        partitionedEdges.writeAsText(outputPathPartitions.replaceAll(":","_"));

        // ### Execute the job in Flink
        //System.out.println(env.getExecutionPlan());
        JobExecutionResult result = env.execute(createJobName(algorithm,k, graphName));

        System.out.println("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute"+"\n");//appends the string to the file
        //System.out.println("The job took " + result.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute"+"\n");

        // Gather statistics for the job
        String statistics = timestamp + "," + graphName + "," + algorithm + "," + k + "," + keyParam + ","
                + result.getNetRuntime((TimeUnit.SECONDS)) + "," + globalPhase + "," + inputPath + "," + windowSizeInMs + "," + wait + "," + folderName;

        if (localRun) {
            File directory = new File(outputPathPartitions);
            File[] partitions = directory.listFiles();
            List<File> fileList = new ArrayList<>();
            int parallelism = 0;
            for (File f : partitions) {
                fileList.add(f);
                parallelism = +1;
            }
            double replicationFactor = Double.parseDouble(new DecimalFormat("##.###").format(new VertexCut(parallelism).calculateVertexCut(fileList)));
            LoadBalanceCalculator lbc = new LoadBalanceCalculator();
            double load = lbc.calculateLoad(fileList);
            double totalNumEdgesInFile = lbc.getTotalNumberOfEdges();
            statistics = statistics + "," + replicationFactor + "," + load + "," + totalNumEdgesInFile;
            System.out.println(statistics);
        }
        //System.out.println("statistics: " + statistics);
        // graphName,algorithm,timestamp,durationInMs,durationInSec,partitions,parallelismModel,inputPath,replicationFactor,load

        try {
            FileWriter fw = new FileWriter(outputStatistics, true); //the true will append the new data
            fw.write(statistics + "\n");//appends the string to the file
            //fw.write("The job took " + result.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute"+"\n");
            fw.close();
        } catch (IOException ioe) {
            System.err.println("IOException: " + ioe.getMessage());
        }

    }

    private static String createJobName(String algorithm, int k, String graphName) {

        String jobName = graphName + " Graph";


        return graphName + " Graph" + " partitioning with " + algorithm + ". parallelism " + k;
    }

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            printInfo = args[0];
            if (printInfo.equals("0")) {
                System.out.println("Debugging mode - more output can be found at logs_job_xyz: " + TEMPGLOBALVARIABLES.printTime);
            }
            inputPath = args[1];
            algorithm = args[2];
            keyParam = Integer.valueOf(args[3]);
            k = Integer.valueOf(args[4]);
            globalPhase = Integer.valueOf(args[5]);
            graphName = args[6];
            outputStatistics = args[7];
            outputPath = args[8];
            windowSizeInMs = Long.parseLong(args[9]);
            wait = Long.parseLong(args[10]);
            testing = args[11];
            if (testing.equals("localTest")) {
                localRun = true;
            }
            if (testing.equals("cluster") && TEMPGLOBALVARIABLES.printModulo < 100000)
                System.out.println("PRINTING MODULO < 100000 !!!!!");
        } else {
            System.out.println("Please provide parameters.");
            System.out.println(" --> Usage: PhasePartitioner <TODO>");
        }
        return true;
}

    public static class PartitionByTag implements Partitioner<Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            return key % numPartitions;
        }
    }

    public static  DataStream<Edge<Integer, NullValue>> getGraphStream1(StreamExecutionEnvironment env) throws IOException {

        return env.readTextFile(inputPath)
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return !value.contains("%");
                    }
                })
                .map(new MapFunction<String, Edge<Integer, NullValue>>() {
                    @Override
                    public Edge<Integer, NullValue> map(String s) throws Exception {
                        String[] fields = s.replaceAll(","," ").split(" ");
                        int src = Integer.parseInt(fields[0]);
                        int trg = Integer.parseInt(fields[1]);
                        return new Edge<>(src, trg, NullValue.getInstance());
                    }
                });

    }

    private static SimpleEdgeStream<Integer, NullValue> getGraphStream(StreamExecutionEnvironment env) {

            return new SimpleEdgeStream<>(env.readTextFile(inputPath)
                    .flatMap(new FlatMapFunction<String, Edge<Integer, NullValue>>() {
                        @Override
                        public void flatMap(String s, Collector<Edge<Integer, NullValue>> out) {
                            String[] fields = s.replaceAll(","," ").split(" ");
                            //String[] fields = s.split("\\s");
                            if (!fields[0].equals("%")) {
                                int src = Integer.parseInt(fields[0]);
                                int trg = Integer.parseInt(fields[1]);
                                out.collect(new Edge<Integer, NullValue>(src, trg, NullValue.getInstance()));
                            }
                        }
                    }), env);
        }

}

