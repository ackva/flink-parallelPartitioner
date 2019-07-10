package org.myorg.quickstart.utils;

import org.apache.flink.graph.Edge;

import java.util.ArrayList;
import java.util.List;

//import org.apache.hadoop.yarn.state.Graph;

public class WinHashStateBig {

    // Essential attributes
    private long key;
    private List<Edge> edgesInState;
    private boolean complete;
    private int size;

    // debugging attributes
    private boolean addedToWatchList;
    private long starttime;
    private long updatetime;
    private long totalTime;
    private boolean updated;
    private String createdBy;
    private long createWatermark;
    private long completeWatermark;

    public void setCompleteWatermark(long completeWatermark) {
        this.completeWatermark = completeWatermark;
    }

    public long getCreateWatermark() {
        return this.createWatermark;
    }

    private long timestampProcessTime;
    private List <Integer> callHistory;
    private int numberOfWatermarks;
    private boolean broadcastArrived;
    private String createdWithElement;

    public WinHashStateBig(long hashValue, Edge edge, long watermark) {
        this.key = hashValue;
        this.edgesInState = new ArrayList<>();
        this.edgesInState.add(edge);
        this.size = -1;
        this.complete = false;
        this.createdBy = "ele";
        this.starttime = System.currentTimeMillis();
        this.updatetime = 0;
        this.createWatermark = watermark;

    }

    public WinHashStateBig(long hashValue, int size, long watermark) {
        this.key = hashValue;
        this.edgesInState = new ArrayList<>();
        this.size = size;
        this.complete = false;
        this.createdBy = "bro";
        this.starttime = System.currentTimeMillis();
        this.updatetime = 0;
        this.createWatermark = watermark;
    }

    // for older versions without watermark
    public WinHashStateBig(long hashValue, Edge edge) {
        this.key = hashValue;
        this.edgesInState = new ArrayList<>();
        this.edgesInState.add(edge);
        this.size = -1;
        this.complete = false;
        this.createdBy = "ele";
        this.starttime = System.currentTimeMillis();
        this.updatetime = 0;

    }

    // for older versions without watermark
    public WinHashStateBig(long hashValue, int size) {
        this.key = hashValue;
        this.edgesInState = new ArrayList<>();
        this.size = size;
        this.complete = false;
        this.createdBy = "bro";
        this.starttime = System.currentTimeMillis();
        this.updatetime = 0;
    }

    public boolean isAddedToWatchList() {
        return addedToWatchList;
    }

    public void setAddedToWatchList(boolean addedToWatchList) {
        this.addedToWatchList = addedToWatchList;
    }

    public boolean addEdge(Edge edge) throws Exception {
        this.edgesInState.add(edge);
        this.updatetime = System.currentTimeMillis() - starttime;
        this.updated = true;
        if(checkComplete())
            return true;
        else
            return false;
    }

    public boolean addBroadcast(int size) {
        this.size = size;
        this.updatetime = System.currentTimeMillis() - starttime;
        this.updated = true;

        if (checkComplete())
            return true;
        else
            return false;

    }

    private boolean checkComplete() {

        if (size > 0 && edgesInState.size() == size) {
            this.totalTime = System.currentTimeMillis() - this.starttime;
            complete = true;
        } else {
            complete = false;
            //System.out.println("incomplete (edges: " + edgesInState.size() + " / " + size + " size");
        }

        return complete;

    }

    public List<Edge> getEdgeList() {
        return this.edgesInState;
    }

    public String printCreateMessage() {
        return "Hash Value/key : " + key + ": call history: " + this.callHistory;
        //return "Hash Value: " + hashValue + " (set by " + this.firstArrivedIn + " with (e/b) " + this.counterEdgesElements + " / " + this.counterEdgesBroadcast + " -- Inital Watermarks | Process " +  timestampWatermark + " | " + timestampProcessTime;
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

    public long getUpdatetime() {
        return updatetime;
    }


    public long getStarttime() {
        return starttime;
    }

    public long getTotalTime() {
        return totalTime;
    }

    public void setComplete() {
        this.complete = true;
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

    public String getCreatedBy() {
        return createdBy;
    }

    public void printUpdateMessage() {
        if (this.complete) {
           // System.out.println("Hash Value: " + hashValue + " complete (" + this.counterEdgesElements + " / " + this.counterEdgesBroadcast + ") call history: " + this.callHistory + " : -- Diff Watermarks | Process || Total WM : --- " +  diffWatermarkTime + " | " + diffProcessTime + " || " + numberOfWatermarks + " ||| " + getDifferencesArrivalTime());
        } else {
            //System.out.println("Hash Value: " + hashValue + " (set by " + this.firstArrivedIn + " with (e/b) " + this.counterEdgesElements + " / " + this.counterEdgesBroadcast + " call history: " + this.callHistory + " : -- Diff Watermarks | Process || Total WM : --- " +  diffWatermarkTime + " | " + diffProcessTime + " || " + numberOfWatermarks);
        }


        //System.out.println("Hash Value: " + hashValue + " (set by " + this.firstArrivedIn + " with (e/b) " + this.counterEdgesElements + " / " + this.counterEdgesBroadcast + " -- Inital Watermarks | Process " +  timestampWatermark + " | " + timestampProcessTime);
    }


        public List<Integer> getCallHistory() {
            return this.callHistory;
        }


}
