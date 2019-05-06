package org.myorg.quickstart.sharedState;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

public class ModelBuilder implements Serializable {

    private HashMap <Integer, HashSet<Integer>> vertexToPartitionMap;
    private String algorithm;

    public ModelBuilder(HashMap<Integer, HashSet<Integer>> vertexToPartitionMap) {
        this.vertexToPartitionMap = vertexToPartitionMap;
    }

    public ModelBuilder(String algorithm, HashMap <Integer, HashSet<Integer>> vertexToPartitionMap) {
        this.vertexToPartitionMap = vertexToPartitionMap;

        switch (algorithm) {
            case "hdrf":
                this.algorithm = "hdrf";
                break;
            case "hash":
                this.algorithm = "hash";
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


    public int choosePartition(EdgeEvent edge) throws Exception {

        int partitionId = -1;

        if (this.algorithm == "byOrigin") {
            partitionId = edge.getEdge().getOriginVertex();
        } else {
            // TODO: Actual algorithm here
            Random rand = new Random();
            partitionId = rand.nextInt(4);
        }

        updateLocalModel(edge, partitionId);

        if (partitionId < 0 ) throw new Exception("Something went wrong with the partitioning algorithm");

        return partitionId;
    }

    public void updateLocalModel(EdgeEvent e, Integer partitionId) {
        Integer[] vertices = new Integer[2];
        vertices[0] = e.getEdge().getOriginVertex();
        vertices[1] = e.getEdge().getDestinVertex();

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
