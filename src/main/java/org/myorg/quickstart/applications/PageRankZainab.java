
package org.myorg.quickstart.applications;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeJoinFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.utils.CustomKeySelector3;
import org.myorg.quickstart.utils.CustomKeySelector4;
import org.myorg.quickstart.utils.StoredObject;
import org.myorg.quickstart.utils.StoredState;

import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.TimeUnit;


public class PageRankZainab {



/**
 * Created by zainababbas on 29/06/2017.
 *
 * SAMPLE INPUT
 *  ß        input path             output path             iterations parallelism
 * 0.85 input/streamInput.txt flinkJobOutput\pageRankZainab 5 2
 *
 **/



    public static void main(String[] args) throws Exception {
        if (!parseParameters(args)) {
            return;
        }
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);



        DataSet<Edge<Double, Double>> data = env.readTextFile(edgesInputPath).map(new MapFunction<String, Edge<Double, Double>>() {

            @Override
            public Edge<Double, Double> map(String s) {
                String[] fields = s.split("\\,");
                double src = Double.parseDouble(fields[0]);
                double trg = Double.parseDouble(fields[1]);
                return new Edge<>(src, trg, 1.0);
            }
        });

        Graph<Double, Double, Double> graph = Graph.fromDataSet(data, new InitVertices(1l), env);

        DataSet<Tuple2<Double, LongValue>> vertexOutDegrees = graph.outDegrees();

        Graph networkWithWeights = graph.joinWithEdgesOnSource(vertexOutDegrees, new InitWeights());

        env.setParallelism(k);

        DataSet<Tuple2<Double, Double>> newVer = vertexOutDegrees.join(networkWithWeights.getVertices()).where(0).equalTo(0).with(new JoinFunction<Tuple2<Double, LongValue>, Tuple2<Double, Double>, Tuple2<Double, Double>>() {
            @Override
            public Tuple2<Double, Double> join(Tuple2<Double, LongValue> doubleLongValueTuple2, Tuple2<Double, Double> doubleDoubleTuple2) throws Exception {
                return new Tuple2<Double, Double>(doubleDoubleTuple2.f0, Double.parseDouble(doubleLongValueTuple2.f1.toString()));
            }
        });

        countV = networkWithWeights.getVertices().count();

        DataSet<Edge<Double, Double>> edges = networkWithWeights.getEdges();

        DeltaIteration<Tuple2<Double, Double>, Tuple2<Double, Double>> iteration = newVer.iterateDelta(newVer, maxIterations, 0);

        DataSet<Tuple2<Double, Double>> changes = iteration.getWorkset().join(edges).where(new CustomKeySelector3(0)).equalTo(new CustomKeySelector4(0))
                .with(new RankMessenger()).withPartitioner(new HDRF<>(new CustomKeySelector3(0), k, 1));
        changes
                .groupBy(0)
                .aggregate(Aggregations.SUM, 1)
                .join(iteration.getSolutionSet()).where(0).equalTo(0)
                .with(new VertexRankUpdater());


        DataSet<Tuple2<Double, Double>> result = iteration.closeWith(changes, changes);


        result.writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE);
        JobExecutionResult resultLog = env.execute("Page rank Hdrf");
        //env.execute("Page rank Hdrf");

        System.out.println("Execution Time: " + resultLog.getNetRuntime(TimeUnit.MILLISECONDS));
        //} else {
        //System.out.println("Printing result to stdout. Use --output to specify output path.");
        //result.print();
        //


    }

    public static final class DuplicateValue implements MapFunction<Double, Tuple2<Double, Double>> {

        @Override
        public Tuple2<Double, Double> map(Double vertex) {

            return new Tuple2<Double, Double>(vertex, 0.0);
        }
    }
    private static class HDRF<T> implements Partitioner<T> {
        private static final long serialVersionUID = 1L;
        CustomKeySelector3 keySelector;
        private double epsilon = 0.0001;
        private double lamda;
        private StoredState currentState;
        private int k = 0;

        public HDRF(CustomKeySelector3 keySelector, int k ,double lamda) {
            this.keySelector = keySelector;
            this.currentState = new StoredState(k);
            this.lamda = lamda;
            this.k=k;
            //System.out.println("createdsfsfsfsdf");
        }

        @Override
        public int partition(Object key,  int numPartitions) {

            long target = 0L;
            try {
                target = (long) Double.parseDouble(keySelector.getValue(key).toString());
            } catch (Exception e) {
                //e.printStackTrace();
                target = 0l;
            }

            long source = (long) Double.parseDouble(key.toString());

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

/*
System.out.print("source"+source);
				System.out.println("target"+target);
				System.out.println("machineid"+machine_id);
*/


            return machine_id;

        }
    }

    private static final class InitVertices implements MapFunction<Double, Double>{

        private double srcId;

        public InitVertices(long srcId) {
            this.srcId = srcId;
        }

        public Double map(Double id) {
            if (id.equals(srcId)) {
                return 0.0;
            }
            else {
                return 0.0;
            }
        }
    }
    public static final class RankMessenger<K> implements JoinFunction<Tuple2<Double, Double>, Edge<Double, Double>, Tuple2<Double, Double>> {
        private  int count =0;

        @Override
        public Tuple2<Double, Double> join(Tuple2<Double, Double> vertex, Edge<Double, Double> edge) throws Exception {

            count++;



            vertex.setField((1.0 / countV), 1);



            return new Tuple2<>(edge.getTarget(),vertex.f1 * edge.getValue());

        }
    }


    public static final class VertexRankUpdater  implements FlatJoinFunction<Tuple2<Double, Double>, Tuple2<Double, Double>, Tuple2<Double, Double>> {
        double rankSum = 0.0;

        @Override
        public void join(Tuple2<Double, Double> candidate, Tuple2<Double, Double> old, Collector<Tuple2<Double, Double>> collector) throws Exception {


            rankSum += candidate.f1;

            rankSum=candidate.f1;


            candidate.f1=(beta * rankSum) + (1 - beta) / countV;
            collector.collect(candidate);

        }
    }


    private static final class InitWeights implements EdgeJoinFunction<Double, LongValue> {
        public Double edgeJoin(Double edgeValue, LongValue inputValue) {
            return edgeValue / (double) inputValue.getValue();
        }
    }

    private static boolean fileOutput = false;

    private static String edgesInputPath = null;

    private static String outputPath = null;

    private static int maxIterations = 5;
    private static double beta;
    private static long countV;
    private static int k = 8;

    private static boolean parseParameters(String[] args) {

        if(args.length > 0) {
            if(args.length != 5) {
                System.err.println("Usage: PageRankDelta" +
                        " <input edges path> <output path> <log> <num iterations> <beta> <no.partitions>");
                return false;
            }

            fileOutput = true;
            beta = Double.parseDouble(args[0]);
            edgesInputPath = args[1];
            outputPath = args[2];
            maxIterations = Integer.parseInt(args[3]);
            k= Integer.parseInt(args[4]);

        } else {

            System.out.println("Usage: PageRankDelta" +
                    " <input edges path> <output path> <log> <num iterations> <beta> <no.partitions>");
        }
        return true;
    }


}