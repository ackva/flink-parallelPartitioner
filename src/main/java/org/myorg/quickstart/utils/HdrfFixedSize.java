package org.myorg.quickstart.utils;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

import java.util.LinkedList;
import java.util.Random;

/**
 * Created by zainababbas on 05/02/2017.
 * Modified by ackva on 07/05/2019
 */


public class HdrfFixedSize<T> implements Partitioner {

    private static final long serialVersionUID = 1L;
    CustomKeySelector keySelector;
    private int epsilon = 1;
    private double lamda;
    private StoredStateFixedSize currentState;
    private int k;

    public HdrfFixedSize(CustomKeySelector keySelector, int k, double lamda, int sampleSize) {
        this.keySelector = keySelector;
        this.currentState = new StoredStateFixedSize(k, sampleSize);
        this.lamda = lamda;
        this.k = k;

    }

/*    public void addToState(Edge edge) {
        long source = Long.parseLong(edge.f0.toString());
        long target = Long.parseLong(edge.f1.toString());
        currentState.getRecord(source);
        currentState.getRecord(target);
    }*/

    public int selectPartition(Edge edge) {

       /* if (this.getCurrentState().getRecord_map().size() >= 250000) {
            System.out.println("inside choose partition");
        }*/

        boolean madeup1 = false;
        boolean madeup2 = false;

        int source = Integer.parseInt(edge.f0.toString());
        int target = Integer.parseInt(edge.f1.toString());

        int machine_id = -1;

        /*StoredObjectFixedSize first_vertex = currentState.getRecord(source);
        StoredObjectFixedSize second_vertex = currentState.getRecord(target);*/
        StoredObjectFixedSize first_vertex;
        StoredObjectFixedSize second_vertex;

        //System.out.println(edge + " ##### ");
        //System.out.println("state now: " + currentState.printState());


        if (currentState.checkIfRecordExits(source)) {
            first_vertex = currentState.getRecord(source);
        } else {
            first_vertex = new StoredObjectFixedSize();
            first_vertex.setDegree(1);
            //System.out.println(source + " - source doesn't exist");
            madeup1 = true;
        }
        if (currentState.checkIfRecordExits(target)) {
            second_vertex = currentState.getRecord(target);
        } else {
            second_vertex = new StoredObjectFixedSize();
            second_vertex.setDegree(1);
            //System.out.println(target + " - target doesn't exist");
            madeup2 = true;

        }

        int min_load = currentState.getMinLoad();
        int max_load = currentState.getMaxLoad();

        LinkedList<Integer> candidates = new LinkedList<Integer>();
        double MAX_SCORE = 0;

        for (int m = 0; m < k; m++) {

            int degree_u;
            int degree_v;
            if (!madeup1)
                degree_u = first_vertex.getDegree() + 1;
            else
                degree_u = 1;
            if (!madeup2) {
                degree_v = second_vertex.getDegree() + 1;
                //System.out.println("target not made up " + target + " degree" + degree_v);
            } else
                degree_v = 1;
            //int degree_v = second_vertex.getDegree() + 1;
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


        if (currentState.getClass() == StoredStateFixedSize.class) {
            StoredStateFixedSize cord_state = (StoredStateFixedSize) currentState;
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

        //3-UPDATE DEGREES  ##### SKIPPING THIS BECAUSE DEGREE IS ALREADY MAINTAINED IN MATCHFUNCTION
        //first_vertex.incrementDegree();
        //second_vertex.incrementDegree();


        //System.out.printPhaseOne("source" + source);
        //System.out.printPhaseOne(target);
        //System.out.println(machine_id);
            /*System.out.printPhaseOne("source"+source);
            System.out.println("target"+target);
            System.out.println("machineid"+machine_id);*/
        //System.out.println("source = " + source + " --- target = " + target + " --> TM " + machine_id);
        return machine_id;
    }


    public StoredStateFixedSize getCurrentState() {
        return currentState;
    }

    @Override
    public int partition(Object key, int numPartitions) {
        return 0;
    }
}

