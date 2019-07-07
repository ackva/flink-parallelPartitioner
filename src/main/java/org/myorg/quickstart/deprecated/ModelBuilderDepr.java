package org.myorg.quickstart.deprecated;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

public class ModelBuilderDepr implements Serializable {

    private HashMap <Integer, HashSet<Integer>> vertexToPartitionMap;
    private String algorithm;

    public ModelBuilderDepr(HashMap<Integer, HashSet<Integer>> vertexToPartitionMap) {
        this.vertexToPartitionMap = vertexToPartitionMap;
    }

    public ModelBuilderDepr(String algorithm, HashMap <Integer, HashSet<Integer>> vertexToPartitionMap) {
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


    public int choosePartition(EdgeEventDepr edge) throws Exception {

        int partitionId = -1;

        if (this.algorithm.equals("byOrigin")) {
            partitionId = edge.getEdge().getOriginVertex();
        } else {
            // TODO: Actual algorithm here
            Random rand = new Random();
            partitionId = rand.nextInt(4);
        }

        if (partitionId < 0 ) throw new Exception("Something went wrong with the partitioning algorithm");

        return partitionId;
    }

}
