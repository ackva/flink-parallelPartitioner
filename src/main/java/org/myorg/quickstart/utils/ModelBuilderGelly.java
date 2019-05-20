package org.myorg.quickstart.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Random;

public class ModelBuilderGelly implements Serializable {

    private HashMap <Long, Long> vertexDegreeMap;
    private String algorithm;
    private Hdrf hdrf;
    private Dbh dbh;
    private HashPartitioner hashPartitioner;
    private CustomKeySelector keySelector;
    private int numOfPartitions;

    public ModelBuilderGelly(String algorithm, HashMap <Long, Long> vertexDegreeMap, Integer k, double lambda) {
        this.vertexDegreeMap = vertexDegreeMap;

        switch (algorithm) {
            case "hdrf":
                this.algorithm = "hdrf";
                Hdrf hdrf = new Hdrf(this.keySelector, k,lambda);
                this.hdrf = hdrf;
                this.numOfPartitions = k;
                break;
            // Hash is not used here. It's in the main function of this program
            /*
                case "hash":
                this.algorithm = "hash";
                this.keySelector = new CustomKeySelector(0);
                this.hashPartitioner = new HashPartitioner(this.keySelector,k);
                break;*/
            default:
                this.algorithm = "random";
                break;
        }
    }

    public ModelBuilderGelly(String algorithm, HashMap <Long, Long> vertexDegreeMap, Integer k) {
        this.vertexDegreeMap = vertexDegreeMap;
        this.algorithm = "dbh";
        this.keySelector = new CustomKeySelector(0);
        Dbh dbh = new Dbh(this.keySelector, k);
        this.dbh = dbh;
    }

    public Hdrf getHdrf() {
        return hdrf;
    }

    public Dbh getDbh() {
        return dbh;
    }

    public HashMap<Long, Long> getVertexDegreeMap() {
        return vertexDegreeMap;
    }

    public int choosePartition(EdgeEventGelly edge) throws Exception {

        int partitionId = -1;

        if (this.algorithm.equals("byOrigin")) {
            partitionId = Integer.parseInt(edge.getEdge().f0.toString());
        } else if (this.algorithm.equals("hash")) {
            partitionId = hashPartitioner.selectPartition(edge.getEdge());
        } else if (this.algorithm.equals("hdrf")) {
            partitionId = hdrf.selectPartition(edge.getEdge());
        } else if (this.algorithm.equals("dbh")) {
            partitionId = dbh.selectPartition(edge.getEdge());
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

/*        // Loop over both vertices and see which one has the higher degree (if equal, the left vertex "wins").
        for (int i = 0; i < 2; i++) {
            HashSet<Integer> currentPartitions = new HashSet();
            if (vertexDegreeMap.containsKey(vertices[i]))
                currentPartitions = vertexDegreeMap.get(vertices[i]);
            currentPartitions.add(partitionId);
            vertexDegreeMap.put(vertices[i],currentPartitions);
        }*/

    }
}
