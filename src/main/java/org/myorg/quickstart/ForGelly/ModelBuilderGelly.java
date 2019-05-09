package org.myorg.quickstart.ForGelly;

import org.apache.flink.graph.Edge;
import org.myorg.quickstart.sharedState.CustomKeySelector;
import org.myorg.quickstart.sharedState.Hdrf;
import org.myorg.quickstart.sharedState.PhasePartitioner;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

public class ModelBuilderGelly implements Serializable {

    private HashMap <Integer, HashSet<Integer>> vertexToPartitionMap;
    private String algorithm;
    private Hdrf hdrf;
    private HashPartitioner hashPartitioner;
    private CustomKeySelector keySelector;

    public ModelBuilderGelly(HashMap<Integer, HashSet<Integer>> vertexToPartitionMap) {
        this.vertexToPartitionMap = vertexToPartitionMap;
    }

    public ModelBuilderGelly(String algorithm, HashMap <Integer, HashSet<Integer>> vertexToPartitionMap) {
        this.vertexToPartitionMap = vertexToPartitionMap;

        switch (algorithm) {
            case "hdrf":
                this.algorithm = "hdrf";
                this.keySelector = new CustomKeySelector(0);
                Hdrf hdrf = new Hdrf<>(this.keySelector, PhasePartitionerGelly.k,PhasePartitionerGelly.lambda);
                this.hdrf = hdrf;
                break;
            case "hash":
                this.algorithm = "hash";
                this.keySelector = new CustomKeySelector(0);
                this.hashPartitioner = new HashPartitioner(this.keySelector,PhasePartitionerGelly.k);
                break;
            case "deterministic":
                this.algorithm = "deterministic";
                break;
            case "byOrigin":
                this.algorithm = "byOrigin";
                break;
            default:
                this.algorithm = "random";
                break;
        }
    }

    public HashMap<Integer, HashSet<Integer>> getVertexToPartitionMap() {
        return vertexToPartitionMap;
    }


    public int choosePartition(EdgeEventGelly edge) throws Exception {

        int partitionId = -1;

        if (this.algorithm.equals("byOrigin")) {
            partitionId = Integer.parseInt(edge.getEdge().f0.toString());
        } else if (this.algorithm.equals("hash")) {
            partitionId = hashPartitioner.selectPartition(edge.getEdge());
        } else if (this.algorithm.equals("hdrf")) {
            partitionId = hdrf.selectPartition(edge.getEdge(),PhasePartitionerGelly.k);
        } else {
            // TODO: Actual algorithm here
            Random rand = new Random();
            partitionId = rand.nextInt(4);
        }

        updateLocalModel(edge, partitionId);

        if (partitionId < 0 ) throw new Exception("Something went wrong with the partitioning algorithm");

        return partitionId;
    }

    public void updateLocalModel(EdgeEventGelly e, Integer partitionId) {
        Integer[] vertices = new Integer[2];
        vertices[0] = Integer.parseInt(e.getEdge().f0.toString());
        vertices[1] = Integer.parseInt(e.getEdge().f1.toString());

        // Loop over both vertices and see which one has the higher degree (if equal, the left vertex "wins").
        for (int i = 0; i < 2; i++) {
            HashSet<Integer> currentPartitions = new HashSet();
            if (vertexToPartitionMap.containsKey(vertices[i]))
                currentPartitions = vertexToPartitionMap.get(vertices[i]);
            currentPartitions.add(partitionId);
            vertexToPartitionMap.put(vertices[i],currentPartitions);
        }

    }
}
