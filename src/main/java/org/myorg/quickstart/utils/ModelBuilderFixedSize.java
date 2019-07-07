package org.myorg.quickstart.utils;

import org.apache.flink.graph.Edge;
import org.myorg.quickstart.DbhParallel.DbhFixedSize;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Random;

public class ModelBuilderFixedSize implements Serializable {

    private HashMap <Integer, Integer> vertexDegreeMap;
    private String algorithm;
    private HdrfFixedSize hdrf;
    private DbhFixedSize dbh;
    private HashPartitioner hashPartitioner;
    private CustomKeySelector keySelector;
    private int numOfPartitions;

    public ModelBuilderFixedSize(String algorithm, int k, double lambda, int sampleSize) {
        this.vertexDegreeMap = vertexDegreeMap;

        switch (algorithm) {
            case "hdrf":
                this.algorithm = "hdrf";
                HdrfFixedSize hdrf = new HdrfFixedSize(this.keySelector, k, lambda, sampleSize);
                this.hdrf = hdrf;
                this.numOfPartitions = k;
                break;
            default:
                this.algorithm = "random";
                break;
        }
    }

    public ModelBuilderFixedSize(String algorithm, Integer k, int sampleSize) {
        this.vertexDegreeMap = vertexDegreeMap;
        this.algorithm = "dbh";
        this.keySelector = new CustomKeySelector(0);
        DbhFixedSize dbh = new DbhFixedSize(this.keySelector, k, sampleSize);
        this.dbh = dbh;
    }

    public HdrfFixedSize getHdrf() {
        return hdrf;
    }

    public DbhFixedSize getDbh() {
        return dbh;
    }

    public HashMap<Integer, Integer> getVertexDegreeMap() {
        return vertexDegreeMap;
    }

    public int choosePartition(Edge<Integer, Long> edge) throws Exception {

        int partitionId = -1;

        if (this.algorithm.equals("hash")) {
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
