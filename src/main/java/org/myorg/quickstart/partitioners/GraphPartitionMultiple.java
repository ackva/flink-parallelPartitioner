package org.myorg.quickstart.partitioners;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;

public class GraphPartitionMultiple {

    public static final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private final static Class<Tuple2<Integer, ArrayList<Integer>>> typedTuple = (Class<Tuple2<Integer, ArrayList<Integer>>>) (Class<?>) Tuple2.class;
    private final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(typedTuple,new GenericTypeInfo<>(Integer.class),new GenericTypeInfo<>(Integer.class));

    // Static variables for debugging, testing, etc.
    public static long windowSizeInMs = 0;
    public static long wait = 0; //public static long sleep = windowSizeInMs/100;
    private static String inputPath = null;
    private static String printInfo = null;
    private static String algorithm = "";
    private static int keyParam = 0;
    private static int globalPhase = 0;
    public static String graphName;
    private static String outputStatistics = null;
    private static String outputPath = null;
    private static String loggingPath = null;
    private static String testing = null;
    public static int k = 2; // parallelism - partitions
    public static double lambda = 1.0;
    public static boolean localRun = false;
    public static int sampleSize = 1_000_000;


    public static void main (String[] args) throws Exception {

        System.out.println(" WORKING WITH TRIAL!!!! --- 2");

        parseParameters(args);

        for (int i = 0; i < 2; i++) {
            GraphPartitionerWinHash2 gpw = new GraphPartitionerWinHash2(
                    printInfo,  inputPath, algorithm, keyParam, k, globalPhase, graphName, outputStatistics, outputPath, windowSizeInMs, wait, sampleSize, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }

/*        int numOfRuns = 3;

        for (int i = 0; i < numOfRuns; i++) {
            GraphPartitionerWinHash2 gpw = new GraphPartitionerWinHash2(
                    printInfo,  inputPath, algorithm, keyParam, 4, 4, graphName, outputStatistics, outputPath, windowSizeInMs, wait, stateDelay, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }

        for (int i = 0; i < numOfRuns; i++) {
            GraphPartitionerWinHash2 gpw = new GraphPartitionerWinHash2(
                    printInfo,  inputPath, algorithm, keyParam, 4, 1, graphName, outputStatistics, outputPath, windowSizeInMs, wait, stateDelay, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }

        for (int i = 0; i < numOfRuns; i++) {
            GraphPartitionerWinHash2 gpw = new GraphPartitionerWinHash2(
                    printInfo,  inputPath, algorithm, keyParam, 2, 2, graphName, outputStatistics, outputPath, windowSizeInMs, wait, stateDelay, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }

        for (int i = 0; i < numOfRuns; i++) {
            GraphPartitionerWinHash2 gpw = new GraphPartitionerWinHash2(
                    printInfo,  inputPath, algorithm, keyParam, 2, 1, graphName, outputStatistics, outputPath, windowSizeInMs, wait, stateDelay, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }

        for (int i = 0; i < numOfRuns; i++) {
            GraphPartitionerWinHash2 gpw = new GraphPartitionerWinHash2(
                    printInfo,  inputPath, algorithm, keyParam, 16, 1, graphName, outputStatistics, outputPath, windowSizeInMs, wait, stateDelay, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }

        for (int i = 0; i < numOfRuns; i++) {
            GraphPartitionerWinHash2 gpw = new GraphPartitionerWinHash2(
                    printInfo,  inputPath, algorithm, keyParam, 16, 16, graphName, outputStatistics, outputPath, windowSizeInMs, wait, stateDelay, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }

        for (int i = 0; i < numOfRuns; i++) {
            GraphPartitionerWinHash2 gpw = new GraphPartitionerWinHash2(
                    printInfo,  inputPath, algorithm, keyParam, 8, 8, graphName, outputStatistics, outputPath, windowSizeInMs, wait, stateDelay, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }

        for (int i = 0; i < numOfRuns; i++) {
            GraphPartitionerWinHash2 gpw = new GraphPartitionerWinHash2(
                    printInfo,  inputPath, algorithm, keyParam, 8, 1, graphName, outputStatistics, outputPath, windowSizeInMs, wait, stateDelay, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }*/


    }

    private static void parseParameters(String[] args) throws Exception {

        if (args.length > 0) {
            printInfo = args[0];
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
            sampleSize = Integer.valueOf(args[11]);
            testing = args[12];
        } else {
            System.out.println("Please provide parameters.");
            System.out.println(" --> Usage: PhasePartitioner <TODO>");
        }
    }

}
