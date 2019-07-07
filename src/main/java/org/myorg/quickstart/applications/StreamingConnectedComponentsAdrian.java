package org.myorg.quickstart.applications;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.partitioners.WinBroIntegratable;
import org.myorg.quickstart.utils.*;

import java.io.*;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by zainababbas on 07/02/2017.
 */
public class StreamingConnectedComponentsAdrian {

    // Static variables for debugging, testing, etc.
    public static long windowSizeInMs = 1;
    public static int wait = 0; //public static long sleep = windowSizeInMs/100;
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
        env.setParallelism(k);

        //DataStream<Edge<Integer, NullValue>> edgeStream;

        if (algorithm.equals("hdrf")) {

            DataStream<Edge<Integer, NullValue>> edgeStream;
            edgeStream = new WinBroIntegratable(env, InputPath, "hdrf", keyParam, k, k, windowSizeInMs, wait)
                    .partitionGraph();
            GraphStream<Integer, NullValue, NullValue> graph = new SimpleEdgeStream<Integer, NullValue>(edgeStream, env);

            DataStream<DisjointSet<Integer>> cc1 = graph.aggregate(new ConnectedComponents<Integer, NullValue>(5000, outputPath));
            cc1.addSink(new DumSink4());

        } else if (algorithm.equals("dbh")) {

            DataStream<Edge<Integer, NullValue>> edgeStream;
            edgeStream = new WinBroIntegratable(env, InputPath, "dbh", keyParam, k, k, windowSizeInMs, wait)
                    .partitionGraph();
            GraphStream<Integer, NullValue, NullValue> graph = new SimpleEdgeStream<Integer, NullValue>(edgeStream, env);

            DataStream<DisjointSet<Integer>> cc1 = graph.aggregate(new ConnectedComponents<Integer, NullValue>(5000, outputPath));
            cc1.addSink(new DumSink4());

        } else if (algorithm.equals("hash")) {

            DataStream<Edge<Long, NullValue>> edgeStreamHash;
            DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
            edgeStreamHash = edges.partitionCustom(new HashPartitioner<>(new CustomKeySelector(0)), new CustomKeySelector<>(0));
            GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<Long, NullValue>(edgeStreamHash, env);
            DataStream<DisjointSet<Long>> cc = graph.aggregate(new ConnectedComponents<Long, NullValue>(5000, outputPath));
            cc.addSink(new DumSink3());

        } else {
            // Zainab's single partitioner HDRF
            env.setParallelism(1);
            DataStream<Edge<Long, NullValue>> edgeStreamHdrfZainab;
            DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
            edgeStreamHdrfZainab = edges.partitionCustom(new HDRF<>(new CustomKeySelector(0),k,1), new CustomKeySelector<>(0));
            GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<Long, NullValue>(edgeStreamHdrfZainab, env);
            DataStream<DisjointSet<Long>> cc = graph.aggregate(new ConnectedComponents<Long, NullValue>(5000, outputPath));
            cc.addSink(new DumSink3());

        }


        // flatten the elements of the disjoint set and print
        // in windows of printWindowTime
/*        cc1.flatMap(new FlattenSet()).keyBy(0)
                .timeWindow(Time.of(1, TimeUnit.MILLISECONDS))
                .fold(new Tuple2<Integer, Integer>(1,1), new IdentityFold());*/
        //cc1.print();

        //DataStream<DisjointSet<Long>> cc = graph.aggregate(new ConnectedComponents<Long, NullValue>(5000));

        JobExecutionResult result1 = env.execute("Connected Components Streaming " + algorithm);
        System.out.println("job 1 execution time"+result1.getNetRuntime(TimeUnit.MILLISECONDS));

/*
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
*/

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
            if (args.length != 5) {
                System.err.println("Usage: Dbh <input edges path> <output path> <log> <partitions> ");
                return false;
            }

            InputPath = args[0];
            outputPath = args[1];
            log = args[2];
            k = (int) Long.parseLong(args[3]);
            algorithm = args[4];
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


    private static class HashPartitioner<T> implements Partitioner<T> {
        private static final long serialVersionUID = 1L;
        CustomKeySelector keySelector;
        private static final int MAX_SHRINK = 100;
        private double seed;
        private int shrink;
        public HashPartitioner(CustomKeySelector keySelector)
        {
            this.keySelector = keySelector;
            System.out.println("createdsfsdfsdfsdf");
            this.seed = Math.random();
            Random r = new Random();
            shrink = r.nextInt(MAX_SHRINK);

        }

        @Override
        public int partition(Object key, int numPartitions) {
            //return MathUtils.murmurHash(key.hashCode()) % numPartitions;
            return Math.abs((int) (  (int) Integer.parseInt(key.toString())*seed*shrink) % numPartitions);


        }

    }



    private static class HDRF<T> implements Partitioner<T> {
        private static final long serialVersionUID = 1L;
        CustomKeySelector keySelector;
        private int epsilon = 1;
        private double lamda;
        private StoredState currentState;
        private int k = 0;

        public HDRF(CustomKeySelector keySelector, int k ,double lamda) {
            this.keySelector = keySelector;
            this.currentState = new StoredState(k);
            this.lamda = lamda;
            this.k=k;


        }

        @Override
        public int partition(Object key,  int numPartitions) {

            long target = 0L;
            try {
                target = (long) keySelector.getValue(key);
            } catch (Exception e) {
                e.printStackTrace();
            }

            long source = (long) key;

            int machine_id = -1;

            StoredObject first_vertex = currentState.getRecordOld(source);
            StoredObject second_vertex = currentState.getRecordOld(target);

            int min_load = currentState.getMinLoad();
            int max_load = currentState.getMaxLoad();

            LinkedList<Integer> candidates = new LinkedList<Integer>();
            double MAX_SCORE = 0;

            for (int m = 0; m < k; m++) {

                int degree_u = first_vertex.getDegree() + 1;
                int degree_v = second_vertex.getDegree() + 1;
                int SUM = degree_u + degree_v;
                double fu = 0;
                double fv = 0;
                if (first_vertex.hasReplicaInPartition(m)) {
                    fu = degree_u;
                    fu /= SUM;
                    fu = 1 + (1 - fu);
                }
                if (second_vertex.hasReplicaInPartition(m)) {
                    fv = degree_v;
                    fv /= SUM;
                    fv = 1 + (1 - fv);
                }
                int load = currentState.getMachineLoad(m);
                double bal = (max_load - load);
                bal /= (epsilon + max_load - min_load);
                if (bal < 0) {
                    bal = 0;
                }
                double SCORE_m = fu + fv + lamda * bal;
                if (SCORE_m < 0) {
                    System.out.println("ERRORE: SCORE_m<0");
                    System.out.println("fu: " + fu);
                    System.out.println("fv: " + fv);
                    System.out.println("GLOBALS.LAMBDA: " + lamda);
                    System.out.println("bal: " + bal);
                    System.exit(-1);
                }
                if (SCORE_m > MAX_SCORE) {
                    MAX_SCORE = SCORE_m;
                    candidates.clear();
                    candidates.add(m);
                } else if (SCORE_m == MAX_SCORE) {
                    candidates.add(m);
                }
            }


            if (candidates.isEmpty()) {
                System.out.println("ERROR: GreedyObjectiveFunction.performStep -> candidates.isEmpty()");
                System.out.println("MAX_SCORE: " + MAX_SCORE);
                System.exit(-1);
            }

            //*** PICK A RANDOM ELEMENT FROM CANDIDATES
            Random r = new Random();
            int choice = r.nextInt(candidates.size());
            machine_id = candidates.get(choice);


            if (currentState.getClass() == StoredState.class) {
                StoredState cord_state = (StoredState) currentState;
                //NEW UPDATE RECORDS RULE TO UPFDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
                if (!first_vertex.hasReplicaInPartition(machine_id)) {
                    first_vertex.addPartition(machine_id);
                    cord_state.incrementMachineLoadVertices(machine_id);
                }
                if (!second_vertex.hasReplicaInPartition(machine_id)) {
                    second_vertex.addPartition(machine_id);
                    cord_state.incrementMachineLoadVertices(machine_id);
                }
            } else {
                //1-UPDATE RECORDS
                if (!first_vertex.hasReplicaInPartition(machine_id)) {
                    first_vertex.addPartition(machine_id);
                }
                if (!second_vertex.hasReplicaInPartition(machine_id)) {
                    second_vertex.addPartition(machine_id);
                }
            }

            Edge e = new Edge<>(source, target, NullValue.getInstance());
            //2-UPDATE EDGES
            currentState.incrementMachineLoad(machine_id, e);

            //3-UPDATE DEGREES
            first_vertex.incrementDegree();
            second_vertex.incrementDegree();
            //System.out.print("source" + source);
            //System.out.print(target);
            //System.out.println(machine_id);
                    /*System.out.print("source"+source);
                    System.out.println("target"+target);
                    System.out.println("machineid"+machine_id);*/

            return machine_id;

        }
    }

}