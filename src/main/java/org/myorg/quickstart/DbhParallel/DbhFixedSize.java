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
    private StoredStateDbh currentState;
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

        boolean madeup1 = false;
        boolean madeup2 = false;

        StoredObjectDbh first_vertex;
        StoredObjectDbh second_vertex;

        if (currentState.checkIfRecordExits(source)) {
            first_vertex = currentState.getRecord(source);
        } else {
            first_vertex = new StoredObjectDbh();
            first_vertex.setDegree(1);
            //System.out.println(source + " - source doesn't exist");
            madeup1 = true;
        }
        if (currentState.checkIfRecordExits(target)) {
            second_vertex = currentState.getRecord(target);
        } else {
            second_vertex = new StoredObjectDbh();
            second_vertex.setDegree(1);
            //System.out.println(target + " - target doesn't exist");
            madeup2 = true;
        }

            int shard_u = Math.abs((int) (source * seed * shrink) % k);
            int shard_v = Math.abs((int) (target * seed * shrink) % k);

            // CHECK THIS HERE
            int degree_u = first_vertex.getDegree();    // removed +1 here
            int degree_v = second_vertex.getDegree();   // removed +1 here

            if (degree_v < degree_u) {
                machine_id = shard_v;
            } else if (degree_u < degree_v) {
                machine_id = shard_u;
            } else { //RANDOM CHOICE
                //*** PICK A RANDOM ELEMENT FROM CANDIDATES
                Random r = new Random();
                int choice = r.nextInt(2);
                if (choice == 0) {
                    machine_id = shard_u;
                } else if (choice == 1) {
                    machine_id = shard_v;
                } else {
                    System.out.println("ERROR IN RANDOM CHOICE DBH");
                    System.exit(-1);
                }
            }



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

