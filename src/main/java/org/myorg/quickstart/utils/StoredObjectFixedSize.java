package org.myorg.quickstart.utils;

import java.io.Serializable;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Created by zainababbas on 05/02/2017.
 */
public class StoredObjectFixedSize implements Serializable {

    private TreeSet<Byte> partitions;
    private int degree;
    private boolean highDegree;

    public StoredObjectFixedSize() {
        partitions = new TreeSet<Byte>();
        this.degree = 0;
    }

    public StoredObjectFixedSize(int degree) {
        partitions = new TreeSet<Byte>();
        this.degree = degree;
    }


    public Iterator<Byte> getPartitions(){
        return partitions.iterator();
    }


    public void addPartition(int m){
        if (m==-1){ System.out.println("ERRORE! record.addPartition(-1)"); System.exit(-1);}
        partitions.add( (byte) m);
    }

    public void addAll(TreeSet<Byte> tree){
        partitions.addAll(tree);
    }


    public boolean hasReplicaInPartition(int m){
        return partitions.contains((byte) m);
    }


    public int getReplicas(){
        return partitions.size();
    }

    public int getDegree() {
        return this.degree;
    }

    public synchronized void setDegree(int degree) {
        this.degree = degree;
    }

    public boolean isHighDegree() {
        return highDegree;
    }

    public synchronized void checkHighDegree(long avgDegree) {
        if (this.degree > avgDegree) {
            this.highDegree = true;
        } else {
            this.highDegree = false;
        }
    }

    public void incrementDegree() {
        this.degree++;
    }

    public static TreeSet<Byte> intersection(StoredObjectFixedSize x, StoredObjectFixedSize y){
        TreeSet<Byte> result = (TreeSet<Byte>) x.partitions.clone();
        result.retainAll(y.partitions);
        return result;
    }

}