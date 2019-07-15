package org.myorg.quickstart.simplifiedForThesis;

import org.apache.flink.graph.Edge;

import java.util.ArrayList;
import java.util.List;

//import org.apache.hadoop.yarn.state.Graph;

public class WindowIdState {

    // Essential attributes
    private long key;
    private List<Edge> edgesInState;
    private boolean complete;
    private int size;

    // debugging attributes
    private boolean updated;
    private boolean createdBy;
    private long createWatermark;

    /*public void setCompleteWatermark(long completeWatermark) {
        this.completeWatermark = completeWatermark;
    }*/

    public long getCreateWatermark() {
        return this.createWatermark;
    }

    public WindowIdState(long hashValue, Edge edge, long watermark) {
        this.key = hashValue;
        this.edgesInState = new ArrayList<>();
        this.edgesInState.add(edge);
        this.size = -1;
        this.complete = false;
        this.createdBy = true;
        this.createWatermark = watermark;

    }

    public WindowIdState(long hashValue, int size, long watermark) {
        this.key = hashValue;
        this.edgesInState = new ArrayList<>();
        this.size = size;
        this.complete = false;
        this.createdBy = false;
        this.createWatermark = watermark;
    }

    // for older versions without watermark
    public WindowIdState(long hashValue, Edge edge) {
        this.key = hashValue;
        this.edgesInState = new ArrayList<>();
        this.edgesInState.add(edge);
        this.size = -1;
        this.complete = false;
        this.createdBy = true;

    }

    // for older versions without watermark
    public WindowIdState(long hashValue, int size) {
        this.key = hashValue;
        this.edgesInState = new ArrayList<>();
        this.size = size;
        this.complete = false;
        this.createdBy = false;
    }


    public boolean addEdge(Edge edge) throws Exception {
        this.edgesInState.add(edge);
        this.updated = true;
        if(checkComplete())
            return true;
        else
            return false;
    }

    public boolean addBroadcast(int size) {
        this.size = size;
        this.updated = true;

        if (checkComplete())
            return true;
        else
            return false;

    }

    public List<Edge> getEdgeList() {
        return this.edgesInState;
    }

    public void setUpdated(boolean updated) {
        this.updated = updated;
    }

    public boolean isUpdated() {
        return updated;
    }

    public void clearEdgeList() {
        this.edgesInState.clear();
    }

    public Integer getSize() {
        return this.size;
    }

    public boolean isComplete() {
        return complete;
    }

    public Long getKey() {
        return this.key;
    }

    public boolean getCreatedBy() {
        return createdBy;
    }

    private boolean checkComplete() {

        if (size > 0 && edgesInState.size() == size) {
            //this.totalTime = System.currentTimeMillis() - this.starttime;
            complete = true;
        } else {
            complete = false;
            //System.out.println("incomplete (edges: " + edgesInState.size() + " / " + size + " size");
        }

        return complete;

    }



}



