package org.myorg.quickstart.DbhParallel;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.myorg.quickstart.utils.StoredObject;
import org.myorg.quickstart.utils.StoredObjectFixedSize;
import org.myorg.quickstart.utils.TEMPGLOBALVARIABLES;
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
    private int vertexCounter;
    private long totalDegreeCount;
    private int sampleSize;
    private double totalInsertTimeBefore;



    //private HashSet<Integer> highDegreeVerices = new HashSet<>();
    private List<Integer> verticesInStateList = new ArrayList<>();
    private long lastCheck = 0L;

    int MAX_LOAD;

    public StoredStateDbh(int k, int sampleSize) {

        record_map = new HashMap<>((int) (sampleSize * 1.5));
        this.sampleSize = sampleSize;
        this.vertexCounter = 0;
        this.totalDegreeCount = 0;

        System.out.println("sample size: " + sampleSize);

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

    public void removeVerticesFromList(HashSet<Integer> toBeRemoved) {
        verticesInStateList.removeAll(toBeRemoved);
    }

    public void incrementMachineLoadVertices(int m) {
        machines_load_vertices[m].incrementAndGet();
    }

    public long getAverageDegree() {
        return (totalDegreeCount/vertexCounter);
    }


    public List<Integer> getVerticesInStateList() {
        return verticesInStateList;
    }

    public void removeRecord(int vertexId) {
        this.totalDegreeCount -= this.getRecord(vertexId).getDegree();
        this.getRecord_map().remove(vertexId);
        this.vertexCounter--;
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

    // TODO: this is a change, compared to Zainab's version!! She adds the object if not existing
    public StoredObjectDbh getRecordOld(Integer x){
        if (!record_map.containsKey(x)){
            record_map.put(x, new StoredObjectDbh());
        }
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

    public int getDegree (StoredObjectFixedSize o) {
        return record_map.get(o).getDegree();
    }

    public long increaseTotalDegree(int degree) {
        totalDegreeCount += degree;
        return totalDegreeCount;
    }

    public long getDegree (StoredObjectDbh o) {
        return record_map.get(o).getDegree();
    }


    public synchronized StoredObjectDbh addRecordWithReservoirSampling(int x, int degree) throws Exception {

            totalDegreeCount += degree;
            vertexCounter++;


            if (record_map.size() > sampleSize) {
                System.out.println(record_map.size() + " out of  ---- AAAAAALAAAAARM " + sampleSize);
            }

            if (record_map.size() < sampleSize) {
                if (!record_map.containsKey(x)){
                    record_map.put(x, new StoredObjectDbh(degree));
                    verticesInStateList.add(x);
                } else {
                    throw new Exception("Entry already exists in state");
                }

                // DEBUG
                if (vertexCounter % 20_000_000 == 0) {
                    totalInsertTimeBefore = System.nanoTime() - lastCheck;
                    lastCheck = System.nanoTime();
                    System.out.println(totalInsertTimeBefore / 1000000 + " ms to add 20,000,000 vertices BEFORE" + vertexCounter);
                }
                // END DEGBUG

            } else {
                //System.out.println(record_map.size()  + " out of " + sampleSize);
                double probabilityToReplace = (double) sampleSize / (double) vertexCounter;
                if (flipCoin(probabilityToReplace)) {
                    Random rand = new Random();
                    long now = System.nanoTime();
                    Object toBeReplaced = verticesInStateList.get(rand.nextInt(verticesInStateList.size()));

                    // DEBUG
/*                if (vertexCounter % 1000 == 0) {
                    totalInsertTimeBefore = System.nanoTime() - lastCheck;
                    lastCheck = System.nanoTime();
                    //System.out.println(totalInsertTimeBefore / 1000000 + " ms to add 1000 vertices AFTER" + vertexCounter);
                }*/
                    // END DEGBUG

/*                if (getRecord_map().size() > (sampleSize - 30000) && getRecord_map().size() < sampleSize) {
                    totalInsertTimeBefore += System.nanoTime() - now;
                    avgInsertTimeBefore = totalInsertTimeBefore / (double) vertexCounter;
                    if (vertexCounter % 10 == 0)
                        System.out.println("checking size before full table took  avg " + avgInsertTimeBefore/1000000 + " ms. counter " + vertexCounter);
                } else if (getRecord_map().size() == (sampleSize)) {
                    vertexCountAfterFull++;
                    totalInsertTimeAfter = totalInsertTimeAfter + (System.nanoTime() - now);
                    avgInsertTimeAfter = totalInsertTimeAfter / (double) vertexCountAfterFull;
                    if (vertexCountAfterFull % 10 == 0)
                        System.out.println("checking size after full table took   " + (double) avgInsertTimeAfter/1000000 + " ms. counter " + vertexCountAfterFull);
                }*/
                    //if (record_map.get(toBeReplaced).isHighDegree()) {

                    if (TEMPGLOBALVARIABLES.keepHighDegree) {
                        probabilityToReplace = 1.0 - (double)record_map.get(toBeReplaced).getDegree()/(double)(vertexCounter-1);
                        if (probabilityToReplace < 0.95)
                            //System.out.println("probabiltity/centraility replaced " + probabilityToReplace);
                            if (flipCoin(probabilityToReplace)) {
                                //System.out.println("probabiltity/centraility replaced " + probabilityToReplace);
                                record_map.remove(toBeReplaced);
                                verticesInStateList.remove(toBeReplaced);
                                record_map.put(x, new StoredObjectDbh(degree));
                                verticesInStateList.add(x);
                            }
                        //System.out.println("replacement done: " + probabilityToReplace);
                    }


                }
                //}
                //}

            }


            return record_map.get(x);
        }

    private boolean flipCoin(double probability) {
        if(Math.random() < probability) {
            return true;
        } else {
            return false;
        }
    }

    @Deprecated
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