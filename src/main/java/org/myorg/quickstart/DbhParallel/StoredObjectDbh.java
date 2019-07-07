package org.myorg.quickstart.DbhParallel;

import java.io.Serializable;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Created by zainababbas on 05/02/2017.
 */
public class StoredObjectDbh implements Serializable {

    private int degree;

    public StoredObjectDbh() {
        degree = 0;
    }

    public StoredObjectDbh(int degree) {
        this.degree = degree;
    }

    public int getDegree() {
        return degree;
    }

    public synchronized void setDegree(int degree) {
        this.degree = degree;
    }

    public void incrementDegree() {
        this.degree++;
    }

}