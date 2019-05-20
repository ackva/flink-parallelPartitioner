package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

import java.util.Random;

public class Dbh<T> implements Partitioner {
    private static final long serialVersionUID = 1L;
    CustomKeySelector keySelector;

    private int k;
    StoredState currentState;
    private static final int MAX_SHRINK = 100;
    private double seed;
    private int shrink;

    public Dbh(CustomKeySelector keySelector, int k)
    {
        this.keySelector = keySelector;
        this.k= k;
        this.currentState = new StoredState(k);
        seed = Math.random();
        Random r = new Random();
        shrink = r.nextInt(MAX_SHRINK);

    }

    public int selectPartition(Edge edge) {

        long source = Long.parseLong(edge.f0.toString());
        long target = Long.parseLong(edge.f1.toString());


        int machine_id = -1;

        StoredObject first_vertex = currentState.getRecord(source);
        StoredObject second_vertex = currentState.getRecord(target);


        int shard_u = Math.abs((int) ( (int) source*seed*shrink) % k);
        int shard_v = Math.abs((int) ( (int) target*seed*shrink) % k);

        int degree_u = first_vertex.getDegree() +1;
        int degree_v = second_vertex.getDegree() +1;

        if (degree_v<degree_u){
            machine_id = shard_v;
        }
        else if (degree_u<degree_v){
            machine_id = shard_u;
        }
        else{ //RANDOM CHOICE
            //*** PICK A RANDOM ELEMENT FROM CANDIDATES
            Random r = new Random();
            int choice = r.nextInt(2);
            if (choice == 0){
                machine_id = shard_u;
            }
            else if (choice == 1){
                machine_id = shard_v;
            }
            else{
                System.out.println("ERROR IN RANDOM CHOICE DBH");
                System.exit(-1);
            }
        }
        //UPDATE EDGES
        Edge e = new Edge<>(source, target, NullValue.getInstance());
        currentState.incrementMachineLoad(machine_id,e);

        //UPDATE RECORDS
        if (currentState.getClass() == StoredState.class){
            StoredState cord_state = (StoredState) currentState;
            //NEW UPDATE RECORDS RULE TO UPFDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
            if (!first_vertex.hasReplicaInPartition(machine_id)){ first_vertex.addPartition(machine_id); cord_state.incrementMachineLoadVertices(machine_id);}
            if (!second_vertex.hasReplicaInPartition(machine_id)){ second_vertex.addPartition(machine_id); cord_state.incrementMachineLoadVertices(machine_id);}
        }
        else{
            //1-UPDATE RECORDS
            if (!first_vertex.hasReplicaInPartition(machine_id)){ first_vertex.addPartition(machine_id);}
            if (!second_vertex.hasReplicaInPartition(machine_id)){ second_vertex.addPartition(machine_id);}
        }

        /*
        //3-UPDATE DEGREES ##### SKIPPING THIS BECAUSE DEGREE IS ALREADY MAINTAINED IN MATCHFUNCTION

        //System.out.print("source"+source);
        //System.out.println("target"+target);
        //System.out.println("machineid"+machine_id);
        first_vertex.incrementDegree();
        second_vertex.incrementDegree();
        */


        return machine_id;
    }

    public StoredState getCurrentState() {
        return currentState;
    }

    @Override
    public int partition(Object key, int numPartitions) {
        return 0;
    }
}

