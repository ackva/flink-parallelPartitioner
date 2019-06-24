package org.myorg.quickstart.utils;

import org.apache.flink.graph.Edge;

import java.util.ArrayList;
import java.util.List;

public class EdgeArrivalChecker {

    private long hashValue;
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
    private boolean complete;
    private String createdWithElement;

    public EdgeArrivalChecker(long hashValue, int sourceFunction, long watermark, long processingTime, int numOfElements, String currentElement) {
        this.numberOfWatermarks = 1;
        this.hashValue = hashValue;
        this.firstArrivedIn = sourceFunction;
        this.timestampWatermark = watermark;
        this.timestampProcessTime = processingTime;
        if (sourceFunction == 0) { // broadcast
            this.counterEdgesBroadcast = numOfElements;
            this.counterEdgesElements = 0;
            //this.firstWatermarkBroadcast = watermark;
            this.firstArrivalBroadcastProcessing = processingTime;
            this.firstArrivalBroadcastWatermark = watermark;
            broadcastArrived = true;
            this.createdWithElement = currentElement;

        }
        if (sourceFunction == 1) { // edge
            this.counterEdgesElements = numOfElements;
            this.counterEdgesBroadcast = 0;
            //this.firstWatermarkElements = watermark;
            this.firstArrivalElementWatermark = processingTime;
            this.firstArrivalElementProcessing = processingTime;
        }
        this.callHistory = new ArrayList<>();
        callHistory.add(sourceFunction);
    }

    public String printCreateMessage() {
        return "Hash Value: " + hashValue + ": call history: " + this.callHistory;
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

    public int getCounterEdgesBroadcast() {
        return counterEdgesBroadcast;
    }

    public int getCounterEdgesElements() {
        return counterEdgesElements;
    }

    public void setComplete() {
        this.complete = true;
    }

    public void updateDifferences(int sourceFunction, long watermark, long processingTime, int edgeCounter, String currentElement) throws Exception {
            this.diffWatermarkTime = watermark - this.timestampWatermark;
            this.diffProcessTime = processingTime - this.timestampProcessTime;

            if (sourceFunction == 1) {
                this.counterEdgesElements++;
                this.lastArrivalElementWatermark = watermark;
                this.lastArrivalElementProcessing = processingTime;
            }
            if (sourceFunction == 0) {
                this.counterEdgesBroadcast =+ this.counterEdgesBroadcast + edgeCounter;
                this.lastArrivalBroadcastWatermark = watermark;
                this.lastArrivalBroadcastProcessing = processingTime;
            }

            if (callHistory.get(0) == 1 && sourceFunction == 0) {
                callHistory.add(sourceFunction);
            } else if (callHistory.get(callHistory.size()-1) == 0 && sourceFunction == 1) {
                    callHistory.add(sourceFunction);
                } else if (callHistory.get(callHistory.size()-1) == 1 && sourceFunction == 1) {
                // do nothing
            } else if (broadcastArrived && sourceFunction == 0) {
                System.out.println("second broadcast arrived at " + hashValue + " created with " + createdWithElement + " -- update request by " + currentElement);
                callHistory.add(sourceFunction);
            } else {
                System.out.println(callHistory + " wants to add " + sourceFunction);
            }

            if (this.counterEdgesBroadcast == this.counterEdgesElements)
                this.complete = true;

        //return "Hash Value: " + hashValue + ": set by " + this.firstArrivedIn + " with (e/b) " + this.counterEdgesElements + " / " + this.counterEdgesBroadcast + " -- Diff Watermarks | Process | Total WM : --- " +  diffWatermarkTime + " | " + diffProcessTime + " | " + numberOfWatermarks;
        //return "Hash Value: " + hashValue + ": call history: " + this.callHistory;

        printUpdateMessage();

        }

    public Long getWatermark () {
        return this.watermark;
    }

    public boolean isComplete() {
        return complete;
    }

    public Long getHashValue() {
        return this.hashValue;
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
