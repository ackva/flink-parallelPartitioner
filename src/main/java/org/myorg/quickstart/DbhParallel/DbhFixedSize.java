package org.myorg.quickstart.DbhParallel;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.myorg.quickstart.utils.CustomKeySelector;
import org.myorg.quickstart.utils.StoredObjectFixedSize;
import org.myorg.quickstart.utils.StoredStateFixedSize;

import java.util.Random;

public class DbhFixedSize<T> implements Partitioner {
    private static final long serialVersionUID = 1L;
    CustomKeySelector keySelector;

    private int k;
    StoredStateDbh currentState;
    private static final int MAX_SHRINK = 100;
    private double seed;
    private int shrink;

    public DbhFixedSize(CustomKeySelector keySelector, int k, int sampleSize)
    {
        this.keySelector = keySelector;
        this.k= k;
        this.currentState = new StoredStateDbh(k, sampleSize);
        seed = Math.random();
        Random r = new Random();
        shrink = r.nextInt(MAX_SHRINK);

    }

    public int selectPartition(Edge<Integer, Long> edge) {

        int source = edge.f0;
        int target = edge.f1;
        //System.out.println("select partition called for " + source + " " + target);

        int machine_id = -1;

        StoredObjectDbh first_vertex = currentState.getRecord(source);
        StoredObjectDbh second_vertex = currentState.getRecord(target);

        int shard_u = Math.abs((int) ( (int) source*seed*shrink) % k);
        int shard_v = Math.abs((int) ( (int) target*seed*shrink) % k);

        // CHECK THIS HERE
        int degree_u = first_vertex.getDegree();    // removed +1 here
        int degree_v = second_vertex.getDegree();   // removed +1 here

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
        /*
        *   ARE THESE THING NEEDED????
        */

        //UPDATE EDGES
        //Edge e = new Edge<>(source, target, NullValue.getInstance());


        //currentState.incrementMachineLoad(machine_id,e);

        //UPDATE RECORDS
/*        if (currentState.getClass() == StoredStateFixedSize.class){
            StoredStateFixedSize cord_state = (StoredStateFixedSize) currentState;
            //NEW UPDATE RECORDS RULE TO UPFDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
            if (!first_vertex.hasReplicaInPartition(machine_id)){ first_vertex.addPartition(machine_id); cord_state.incrementMachineLoadVertices(machine_id);}
            if (!second_vertex.hasReplicaInPartition(machine_id)){ second_vertex.addPartition(machine_id); cord_state.incrementMachineLoadVertices(machine_id);}
        }
        else{
            //1-UPDATE RECORDS
            if (!first_vertex.hasReplicaInPartition(machine_id)){ first_vertex.addPartition(machine_id);}
            if (!second_vertex.hasReplicaInPartition(machine_id)){ second_vertex.addPartition(machine_id);}
        }*/

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

    public StoredStateDbh getCurrentState() {
        return currentState;
    }

    @Override
    public int partition(Object key, int numPartitions) {
        return 0;
    }
}

