package org.myorg.quickstart.DbhParallel;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.myorg.quickstart.utils.StoredObject;
import scala.Int;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zainababbas on 06/02/2017.
 */
public class StoredStateDbh implements Serializable{

    private HashMap<Integer, StoredObjectDbh> record_map;
    private AtomicInteger[] machines_load_edges;
    private AtomicInteger[] machines_load_vertices;

    int MAX_LOAD;

    public StoredStateDbh(int k, int sampleSize) {

        record_map = new HashMap<>();
        machines_load_edges = new AtomicInteger[k];
        for (int i = 0; i<machines_load_edges.length;i++){
            machines_load_edges[i] = new AtomicInteger(0);
        }
        machines_load_vertices = new AtomicInteger[k];
        for (int i = 0; i<machines_load_vertices.length;i++){
            machines_load_vertices[i] = new AtomicInteger(0);
        }
        MAX_LOAD = 0;

    }

    public void incrementMachineLoadVertices(int m) {
        machines_load_vertices[m].incrementAndGet();
    }

    public int[] getMachines_loadVertices() {
        int [] result = new int[machines_load_vertices.length];
        for (int i = 0; i<machines_load_vertices.length;i++){
            result[i] = machines_load_vertices[i].get();
        }
        return result;
    }

    public HashMap<Integer, StoredObjectDbh> getRecord_map() {
        return record_map;
    }

    // TODO: this is a change, compared to Zainab's version!! She adds the object if not existing
    public StoredObjectDbh getRecord(int x){
        return record_map.get(x);
    }


    public boolean checkIfRecordExits(int x) {
        if (!record_map.containsKey(x)) {
            return false;
        }
        else {
            return true;
        }
    }

    public long getDegree (StoredObjectDbh o) {
        return record_map.get(o).getDegree();
    }

    public StoredObjectDbh addRecordWithDegree(int x, int degree) throws Exception {
        if (!record_map.containsKey(x)){
            record_map.put(x, new StoredObjectDbh(degree));
        } else {
            throw new Exception("Entry already exists in state");
        }
        return record_map.get(x);
    }

    public int getNumVertices(){
        return record_map.size();
    }

    public synchronized int getMachineLoad(int m) {
        return machines_load_edges[m].get();
    }

    public synchronized int getMachineVerticesLoad(int m) {
        return machines_load_vertices[m].get();
    }

    public synchronized void incrementMachineLoad(int m, Edge e) {
        int new_value = machines_load_edges[m].incrementAndGet();
        if (new_value>MAX_LOAD){
            MAX_LOAD = new_value;
        }
        //if (GLOBALS.OUTPUT_FILE_NAME!=null){
        //	out.write(e+": "+m+"\n");
        //	}
    }


    public int[] getMachines_load() {
        int [] result = new int[machines_load_edges.length];
        for (int i = 0; i<machines_load_edges.length;i++){
            result[i] = machines_load_edges[i].get();
        }
        return result;
    }


    public synchronized int getMinLoad() {
        int MIN_LOAD = Integer.MAX_VALUE;
        for (AtomicInteger load : machines_load_edges) {
            int loadi = load.get();
            if (loadi<MIN_LOAD){
                MIN_LOAD = loadi;
            }
        }
        return MIN_LOAD;
    }


    public int getMaxLoad() {
        return MAX_LOAD;
    }

/*    public SortedSet<Integer> getVertexIds() {
        //if (GLOBALS.OUTPUT_FILE_NAME!=null){ out.close(); }
        return new TreeSet<Long>(record_map.keySet());
    }*/

    public List<Tuple2> printState() {
        List<Tuple2> stateList = new ArrayList<>();
        for (Map.Entry<Integer, StoredObjectDbh> entry: record_map.entrySet()) {
            stateList.add(new Tuple2<>(entry.getKey(),entry.getValue().getDegree()));
        }
        return stateList;
    }

}