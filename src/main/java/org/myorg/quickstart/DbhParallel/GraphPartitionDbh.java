package org.myorg.quickstart.DbhParallel;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.myorg.quickstart.partitioners.GraphPartitionerReservoirSampling;

import java.util.ArrayList;

public class GraphPartitionDbh {

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



/*
        GraphPartitionerWinHash gpw = new GraphPartitionerWinHash(
                printInfo,  inputPath, algorithm, keyParam, k, globalPhase, graphName+"WinHash", outputStatistics, outputPath, windowSizeInMs, wait, (int) sampleSize, testing
        );
        gpw.partitionGraph();
        Thread.sleep(1000);
        System.out.println(" ############ ");
*/

/*
        System.out.println(" ############ SAMPLE SIZE BIG ENOUGH");

        GraphPartitionerReservoirSampling gpw = new GraphPartitionerReservoirSampling(
                printInfo, inputPath, algorithm, keyParam, k, globalPhase, graphName + "_resSampling_noHigh", outputStatistics, outputPath, windowSizeInMs, wait, (int) 5000000, testing
        );
        gpw.partitionGraph();
        Thread.sleep(1000);
        System.out.println(" ############ ");
*/


/*        System.out.println(" ############ SAMPLE SIZE " + sampleSize);

        GraphPartitionerReservoirSampling gpw0 = new GraphPartitionerReservoirSampling(
                printInfo, inputPath, algorithm, keyParam, k, globalPhase, graphName + "_resSampling_noHigh", outputStatistics, outputPath, windowSizeInMs, wait, sampleSize, testing
        );
        gpw0.partitionGraph();
        Thread.sleep(1000);
        System.out.println(" ############ ");*/



/*        for (int j = 1600; j > 0; j--) {
            //double sampleSizeRun = (double) sampleSize * ((10.0 - j)/10.0);
            int sampleSizeRun = j;
            j -= 99;
            System.out.println("s: " + (int) sampleSizeRun);
            for (int i = 0; i < 2; i++) {
                System.out.println("Run: " + i);
                GraphPartitionerReservoirSampling gpw = new GraphPartitionerReservoirSampling(
                        printInfo,  inputPath, algorithm, keyParam, k, globalPhase, graphName+"_resSampling_noHigh", outputStatistics, outputPath, windowSizeInMs, wait, sampleSizeRun, testing
                );
                gpw.partitionGraph();
                Thread.sleep(1000);
                System.out.println(" ############ ");

            }
        }*/





/*
        System.out.println(" ############ SAMPLE SIZE 100");

        GraphPartitionerReservoirSampling gpw1 = new GraphPartitionerReservoirSampling(
                printInfo, inputPath, algorithm, keyParam, k, globalPhase, graphName + "_resSampling_noHigh", outputStatistics, outputPath, windowSizeInMs, wait, (int) 100, testing
        );
        gpw1.partitionGraph();
        Thread.sleep(1000);
        System.out.println(" ############ ");

        System.out.println(" ############ SAMPLE SIZE 200");

        GraphPartitionerReservoirSampling gpw2 = new GraphPartitionerReservoirSampling(
                printInfo, inputPath, algorithm, keyParam, k, globalPhase, graphName + "_resSampling_noHigh", outputStatistics, outputPath, windowSizeInMs, wait, (int) 200, testing
        );
        gpw2.partitionGraph();
        Thread.sleep(1000);
        System.out.println(" ############ ");

        System.out.println(" ############ SAMPLE SIZE 500");

        GraphPartitionerReservoirSampling gpw3 = new GraphPartitionerReservoirSampling(
                printInfo, inputPath, algorithm, keyParam, k, globalPhase, graphName + "_resSampling_noHigh", outputStatistics, outputPath, windowSizeInMs, wait, (int) 500, testing
        );
        gpw3.partitionGraph();
        Thread.sleep(1000);
        System.out.println(" ############ ");

        System.out.println(" ############ SAMPLE SIZE 1000");

        GraphPartitionerReservoirSampling gpw4 = new GraphPartitionerReservoirSampling(
                printInfo, inputPath, algorithm, keyParam, k, globalPhase, graphName + "_resSampling_noHigh", outputStatistics, outputPath, windowSizeInMs, wait, (int) 1000, testing
        );
        gpw4.partitionGraph();
        Thread.sleep(1000);
        System.out.println(" ############ ");

        System.out.println(" ############ SAMPLE SIZE 5000");

        GraphPartitionerReservoirSampling gpw5 = new GraphPartitionerReservoirSampling(
                printInfo, inputPath, algorithm, keyParam, k, globalPhase, graphName + "_resSampling_noHigh", outputStatistics, outputPath, windowSizeInMs, wait, (int) 5000, testing
        );
        gpw5.partitionGraph();
        Thread.sleep(1000);
        System.out.println(" ############ ");
*/



















/*        for (int j = 0; j < 15; j++) {
            //double sampleSizeRun = (double) sampleSize * ((10.0 - j)/10.0);
            double sampleSizeRun = (double) sampleSize * ((10.0 - j)/10.0);
            System.out.println("s: " + (int) sampleSizeRun);
            for (int i = 0; i < 2; i++) {
                System.out.println("Run: " + i);
                GraphPartitionerReservoirSampling gpw = new GraphPartitionerReservoirSampling(
                        printInfo,  inputPath, algorithm, keyParam, k, globalPhase, graphName+"_resSampling_noHigh", outputStatistics, outputPath, windowSizeInMs, wait, (int) sampleSizeRun, testing
                );
                gpw.partitionGraph();
                Thread.sleep(1000);
                System.out.println(" ############ ");

            }
        }*/


/*        System.out.println(" ############ ");
        System.out.println("Regular - without sampling");
        for (int i = 0; i < 3; i++) {
            GraphPartitionerWinHash gpw = new GraphPartitionerWinHash(
                    printInfo,  inputPath, algorithm, keyParam, k, globalPhase, graphName, outputStatistics, outputPath, windowSizeInMs, wait, sampleSize, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }*/


        System.out.println(" ############ ");




/*        int numOfRuns = 3;

        for (int i = 0; i < numOfRuns; i++) {
            GraphPartitionerReservoirSampling gpw = new GraphPartitionerReservoirSampling(
                    printInfo,  inputPath, algorithm, keyParam, 4, 4, graphName, outputStatistics, outputPath, windowSizeInMs, wait, stateDelay, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }

        for (int i = 0; i < numOfRuns; i++) {
            GraphPartitionerReservoirSampling gpw = new GraphPartitionerReservoirSampling(
                    printInfo,  inputPath, algorithm, keyParam, 4, 1, graphName, outputStatistics, outputPath, windowSizeInMs, wait, stateDelay, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }

        for (int i = 0; i < numOfRuns; i++) {
            GraphPartitionerReservoirSampling gpw = new GraphPartitionerReservoirSampling(
                    printInfo,  inputPath, algorithm, keyParam, 2, 2, graphName, outputStatistics, outputPath, windowSizeInMs, wait, stateDelay, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }

        for (int i = 0; i < numOfRuns; i++) {
            GraphPartitionerReservoirSampling gpw = new GraphPartitionerReservoirSampling(
                    printInfo,  inputPath, algorithm, keyParam, 2, 1, graphName, outputStatistics, outputPath, windowSizeInMs, wait, stateDelay, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }

        for (int i = 0; i < numOfRuns; i++) {
            GraphPartitionerReservoirSampling gpw = new GraphPartitionerReservoirSampling(
                    printInfo,  inputPath, algorithm, keyParam, 16, 1, graphName, outputStatistics, outputPath, windowSizeInMs, wait, stateDelay, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }

        for (int i = 0; i < numOfRuns; i++) {
            GraphPartitionerReservoirSampling gpw = new GraphPartitionerReservoirSampling(
                    printInfo,  inputPath, algorithm, keyParam, 16, 16, graphName, outputStatistics, outputPath, windowSizeInMs, wait, stateDelay, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }

        for (int i = 0; i < numOfRuns; i++) {
            GraphPartitionerReservoirSampling gpw = new GraphPartitionerReservoirSampling(
                    printInfo,  inputPath, algorithm, keyParam, 8, 8, graphName, outputStatistics, outputPath, windowSizeInMs, wait, stateDelay, testing
            );
            gpw.partitionGraph();
            Thread.sleep(1000);
        }

        for (int i = 0; i < numOfRuns; i++) {
            GraphPartitionerReservoirSampling gpw = new GraphPartitionerReservoirSampling(
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
