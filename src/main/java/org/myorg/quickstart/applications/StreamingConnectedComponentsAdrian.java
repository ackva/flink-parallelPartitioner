package org.myorg.quickstart.applications;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.EdgesFold;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.library.ConnectedComponents;
import org.apache.flink.graph.streaming.summaries.DisjointSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.partitioners.WinBroIntegratable;
import org.myorg.quickstart.utils.*;

import java.io.*;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

//import org.apache.flink.graph.streaming.WindowGraphAggregation;


/**
 * Created by zainababbas on 07/02/2017.
 */
public class StreamingConnectedComponentsAdrian {

    // Static variables for debugging, testing, etc.
    public static long windowSizeInMs = 1;
    //public static int printModulo = 500_000;
    public static int wait = 0; //public static long sleep = windowSizeInMs/100;
    //private static String inputPath = InputPath;
    private static String printInfo = null;
    private static String algorithm = "";
    public static int keyParam = 0;
    private static int globalPhase = 0;
    public static String graphName;
    private static String outputStatistics = null;
    private static String loggingPath = null;
    private static String testing = null;
    public static double lambda = 1.0;
    public static boolean localRun = false;
    public static int stateDelay = 0;


    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
        env.setParallelism(k);

        //DataStream<Edge<Long, NullValue>> partitionesedges = edges.partitionCustom(new DbhPartitioner<>(new CustomKeySelector<>(0),k), new CustomKeySelector<>(0));
        //partitionesedges.addSink(new DumSink2());
        //GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(edges.partitionCustom(new DbhPartitioner<>(new CustomKeySelector(0),k), new CustomKeySelector<>(0)),env);

        DataStream<Edge<Integer,NullValue>> edgeStream = new WinBroIntegratable(env, InputPath, "hdrf", keyParam, k, k, windowSizeInMs, wait)
                .partitionGraph();

        GraphStream<Integer, NullValue, NullValue> graph2 = new SimpleEdgeStream<Integer, NullValue>(edgeStream, env);

        //GraphStream<Long, NullValue, NullValue> graph1 = new SimpleEdgeStream<Long, NullValue>(edgeStream);

        DataStream<DisjointSet<Integer>> cc1 = graph2.aggregate(new ConnectedComponents<Integer, NullValue>(5000));
        cc1.addSink(new DumSink4());
        // flatten the elements of the disjoint set and print
        // in windows of printWindowTime
        cc1.flatMap(new FlattenSet()).keyBy(0)
                .timeWindow(Time.of(1, TimeUnit.MILLISECONDS))
                .fold(new Tuple2<Integer, Integer>(1,1), new IdentityFold());
        cc1.print();

        //DataStream<DisjointSet<Long>> cc = graph.aggregate(new ConnectedComponents<Long, NullValue>(5000));


        //JobExecutionResult result = env.execute("My Flink Job");
		//System.out.println("job 1 execution time"+result.getNetRuntime(TimeUnit.MILLISECONDS));
		//GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(partitionesedges,env);
		//DataStream<DisjointSet<Long>> cc = graph.aggregate(new ConnectedComponents<Long, NullValue>(5000));
		//cc.addSink(new DumSink3());
		// flatten the elements of the disjoint set and print
		// in windows of printWindowTime
		//cc.flatMap(new FlattenSet()).keyBy(0)
		//		.timeWindow(Time.of(1, TimeUnit.MILLISECONDS))
		//		.fold(new Tuple2<Long, Long>(0l, 0l), new StreamingConnectedComponents.IdentityFold());
		//cc.print();
        JobExecutionResult result1 = env.execute("My Flink Job");
        System.out.println("job 1 execution time"+result1.getNetRuntime(TimeUnit.MILLISECONDS));

        try {
            FileWriter fw = new FileWriter(log, true); //the true will append the new data
            //	fw.write("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute" + "\n");//appends the string to the file
            //	fw.write("The job took " + result.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute" + "\n");
            fw.write("The job 1 took " + result1.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute" + "\n");//appends the string to the file
            fw.write("The job 1 took " + result1.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute" + "\n");
            fw.close();
        } catch (IOException ioe) {
            System.err.println("IOException: " + ioe.getMessage());
        }

    }


    public static final class IdentityFold implements FoldFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        public Tuple2<Integer, Integer> fold(Tuple2<Integer, Integer> accumulator, Tuple2<Integer, Integer> value) throws Exception {
            return value;
        }
    }

    public static class PartitionByTag implements Partitioner<Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            return key % numPartitions;
        }
    }

    private static String InputPath = null;
    private static String outputPath = null;
    private static String log = null;
    private static int k = 0;
    private static int count = 0;
    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if (args.length != 4) {
                System.err.println("Usage: Dbh <input edges path> <output path> <log> <partitions> ");
                return false;
            }

            InputPath = args[0];
            outputPath = args[1];
            log = args[2];
            k = (int) Long.parseLong(args[3]);
        } else {
            System.out.println("Executing example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println(" Usage: Dbh <input edges path> <output path> <log> <partitions>");
        }
        return true;
    }


    public static final class FlattenSet implements FlatMapFunction<DisjointSet<Integer>, Tuple2<Integer, Integer>> {

        private Tuple2<Integer, Integer> t = new Tuple2<>();

        @Override
        public void flatMap(DisjointSet<Integer> set, Collector<Tuple2<Integer, Integer>> out) {
            for (Integer vertex : set.getMatches().keySet()) {
                Integer parent = set.find(vertex);
                t.setField(vertex, 0);
                t.setField(parent, 1);
                out.collect(t);
            }
        }
    }

    public static  DataStream<Edge<Long, NullValue>> getGraphStream(StreamExecutionEnvironment env) throws IOException {

        return env.readTextFile(InputPath)
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return !value.contains("%");
                    }
                })
                .map(new MapFunction<String, Edge<Long, NullValue>>() {
                    @Override
                    public Edge<Long, NullValue> map(String s) throws Exception {
                        String[] fields = s.replaceAll(","," ").split(" ");
                        long src = Long.parseLong(fields[0]);
                        long trg = Long.parseLong(fields[1]);
                        return new Edge<>(src, trg, NullValue.getInstance());
                    }
                });

    }

}
