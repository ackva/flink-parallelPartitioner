package org.myorg.quickstart.utils;

import org.apache.flink.graph.Edge;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Random;

public class ModelBuilderGellyLong implements Serializable {

    private HashMap <Integer, Integer> vertexDegreeMap;
    private String algorithm;
    private Hdrf hdrf;
    private Dbh dbh;
    private HashPartitioner hashPartitioner;
    private CustomKeySelector keySelector;
    private int numOfPartitions;

    public ModelBuilderGellyLong(String algorithm, HashMap <Integer, Integer> vertexDegreeMap, Integer k, double lambda) {
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

    public ModelBuilderGellyLong(String algorithm, HashMap <Integer, Integer> vertexDegreeMap, Integer k) {
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

    public HashMap<Integer, Integer> getVertexDegreeMap() {
        return vertexDegreeMap;
    }

    public int choosePartition(Edge<Integer, Long> edge) throws Exception {

        int partitionId = -1;

        if (this.algorithm.equals("byOrigin")) {
            partitionId = edge.f0;
        } else if (this.algorithm.equals("hash")) {
            partitionId = hashPartitioner.selectPartition(edge);
        } else if (this.algorithm.equals("hdrf")) {
            partitionId = hdrf.selectPartition(edge);
        } else if (this.algorithm.equals("dbh")) {
            partitionId = dbh.selectPartition(edge);
        } else {
            // TODO: Actual algorithm here
            Random rand = new Random();
            partitionId = rand.nextInt(4);
        }

        if (partitionId < 0 ) throw new Exception("Something went wrong with the partitioning algorithm");

        return partitionId;
    }

}
