package org.myorg.quickstart.utils;

import org.apache.flink.graph.Edge;
//import org.apache.hadoop.yarn.state.Graph;

import java.util.ArrayList;
import java.util.List;

public class WinHashState {

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

    private long watermark;
    private long diffWatermarkTime;
    private long diffProcessTime;
    private List<Edge> edgesArrived;
    private long firstArrivalBroadcastWatermark;
    private long firstArrivalBroadcastProcessing;
    private long firstArrivalElementWatermark;
    private long firstArrivalElementProcessing;
    private long lastArrivalBroadcastWatermark;
    private long lastArrivalBroadcastProcessing;
    private long lastArrivalElementWatermark;
    private long lastArrivalElementProcessing;
    private int firstArrivedIn;
    private int counterEdgesBroadcast;
    private int counterEdgesElements;
    private long firstWatermarkBroadcast;
    private long firstWatermarkElements;
    private long timestampWatermark;
    private long timestampProcessTime;
    private List <Integer> callHistory;
    private int numberOfWatermarks;
    private boolean broadcastArrived;
    private String createdWithElement;

    public WinHashState(long hashValue, Edge edge) {
        this.key = hashValue;
        this.edgesInState = new ArrayList<>();
        this.edgesInState.add(edge);
        this.size = -1;
        this.complete = false;
        this.createdBy = "ele";
        this.starttime = System.currentTimeMillis();
        this.updatetime = 0;
    }

    public WinHashState(long hashValue, int size) {
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

    public String getDifferencesArrivalTime() {

        long diffWatermark;
        long diffProcessing;
        if (firstArrivedIn == 0) {
            diffWatermark = lastArrivalElementWatermark - firstArrivalBroadcastWatermark;
            diffProcessing = lastArrivalElementProcessing - firstArrivalBroadcastProcessing;
        } else {
            diffWatermark = lastArrivalBroadcastWatermark - firstArrivalElementWatermark;
            diffProcessing = lastArrivalBroadcastProcessing - firstArrivalElementProcessing;
        }



        return "Diff Watermark: " + diffWatermark + " || Diff Processing: " + diffProcessing;
    }

    public void setUpdated(boolean updated) {
        this.updated = updated;
    }

    public boolean isUpdated() {
        return updated;
    }

    public int getCounterEdgesBroadcast() {
        return counterEdgesBroadcast;
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

    public int getCounterEdgesElements() {
        return counterEdgesElements;
    }

    public void setComplete() {
        this.complete = true;
    }

    public Integer getSize() {
        return this.size;
    }


    public Long getWatermark () {
        return this.watermark;
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
