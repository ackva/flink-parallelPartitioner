package org.myorg.quickstart.utils;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.graph.Edge;
import org.apache.flink.util.MathUtils;

/**
 * @author Adrian Ackva
 * Copied from Zainab Abbas and slightly modified
 * @param <T>
 */

public class HashPartitioner<T> implements Partitioner<T> {

    private static final long serialVersionUID = 1L;
    CustomKeySelector keySelector;
    /*	private double seed;
        private int shrink;
        private static final int MAX_SHRINK = 100; */
        private int numOfPartitions;

    public HashPartitioner(Integer numOfPartitions) {
        //this.keySelector = keySelector;
        //	this.seed = Math.random();
        //	Random r = new Random();
        //	shrink = r.nextInt(MAX_SHRINK);
        this.numOfPartitions = numOfPartitions;
    }

    public int selectPartition(Edge edge) {
        //long source = (long) key;
        //return Math.abs((int) ( (int) (source)*seed*shrink) % k);
        return MathUtils.murmurHash(edge.f0.hashCode()) % this.numOfPartitions;
    }

    @Override
    public int partition(Object key, int numOfPartitions) {
    //long source = (long) key;
    //return Math.abs((int) ( (int) (source)*seed*shrink) % k);
    return MathUtils.murmurHash(key.hashCode()) % numOfPartitions;


    }

}
