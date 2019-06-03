package org.myorg.quickstart.partitioners;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.myorg.quickstart.utils.CustomKeySelector;
import org.myorg.quickstart.utils.StoredObject;
import org.myorg.quickstart.utils.StoredState;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;

/**
 * Created by zainababbas on 05/02/2017.
 */
public class HdrfZainab {


    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);

        edges.partitionCustom(new HDRF<>(new CustomKeySelector(0),k,lamda), new CustomKeySelector<>(0)).writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(k);
        //edges.partitionCustom(new HDRF<>(new CustomKeySelector(0),k,lamda), new CustomKeySelector<>(0))
        //		.addSink(new TimestampingSink(outputPath)).setParallelism(k);

        JobExecutionResult result = env.execute("My Flink Job");

/*        try {
            FileWriter fw = new FileWriter(log, true); //the true will append the new data
            fw.write("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute"+"\n");//appends the string to the file
            fw.write("The job took " + result.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute"+"\n");
            fw.close();
        } catch (IOException ioe) {
            System.err.println("IOException: " + ioe.getMessage());
        }*/

        System.out.println("abc");
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************



    private static String InputPath = null;
    private static String outputPath = null;
    private static String log = null;
    private static int k = 0;
    private static double lamda = 0.0;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if (args.length != 5) {
                System.err.println("Usage: hdrf <input edges path> <output path> <log> <partitions> <lamda> ");
                return false;
            }

            InputPath = args[0];
            outputPath = args[1];
            log = args[2];
            k = (int) Long.parseLong(args[3]);
            lamda = Double.parseDouble(args[4]);
        } else {
            System.out.println("Executing example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println(" Usage: hdrf <input edges path> <output path> <log> <partitions> <lamda>");
        }
        return true;
    }



    ///////code for partitioner/////////
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
            //System.out.printPhaseOne("source" + source);
            //System.out.printPhaseOne(target);
            //System.out.println(machine_id);
				/*System.out.printPhaseOne("source"+source);
				System.out.println("target"+target);
				System.out.println("machineid"+machine_id);*/

            return machine_id;

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
                        String[] fields = s.split(" ");
                        long src = Long.parseLong(fields[0]);
                        long trg = Long.parseLong(fields[1]);
                        return new Edge<>(src, trg, NullValue.getInstance());
                    }
                });

    }
}