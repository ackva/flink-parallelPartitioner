package org.myorg.quickstart.sharedState;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestingGraph {

    private List<EdgeSimple> edges;
    private List<EdgeEvent> edgeEvents;

    public TestingGraph(String characteristic, int graphSize) throws Exception {
        switch (characteristic) {
            case "one":
                generateGraphOneToAny(graphSize);
                break;
            case "two":
                generateGraphOneTwoToAny(graphSize);
                break;
            case "three":
                generateGraphOneFiveToFive();
                break;
            case "modulo10":
                generateGraphModulo10(graphSize);
                break;
            default:
                generateRandomGraph(graphSize);
                break;
        }

        ArrayList<EdgeEvent> edgeEventList = new ArrayList<>();
        for (EdgeSimple e: this.getEdges()) {
//            System.out.println(e);
            edgeEventList.add(new EdgeEvent(e));
            Thread.sleep(100);
        }
        this.edgeEvents = edgeEventList;
    }

    // empty graph -- needs to call a "generate" functions after
    public TestingGraph() {
        }

    public List<EdgeSimple> getEdges() {
        return edges;
    }

    public List<EdgeEvent> getEdgeEvents() {
        return edgeEvents;
    }

    public void generateGraphOneToAny(int numbEdges) {
        List<EdgeSimple> edgeList = new ArrayList<>();
        for (int i = 1; i < numbEdges+1; i++) {
            edgeList.add(new EdgeSimple(1,i+1));
        }
        this.edges = edgeList;
    }

    public void generateGraphOneTwoToAny(int numbEdges) {
        List<EdgeSimple> edgeList = new ArrayList<>();
        for (int i = 1; i < numbEdges/2+1; i++) {
            edgeList.add(new EdgeSimple(1,i+1));
        }
        for (int i = numbEdges/2+1; i < numbEdges+1; i++) {
            edgeList.add(new EdgeSimple(2,i+1));
        }
        this.edges = edgeList;
    }


    public void generateGraphModulo10(int numbEdges) throws Exception {
        if (numbEdges%10 != 0) {
            throw new Exception("Provide a graphSize divisible by 10");
        }
        List<EdgeSimple> edgeList = new ArrayList<>();
        int originEdge = 1;
        for (int i = 1; i < numbEdges; i++) {
            if (i %10 == 0)
                originEdge++;
            edgeList.add(new EdgeSimple(originEdge,originEdge + i));
        }
        this.edges = edgeList;
    }


    public void generateGraphOneFiveToFive() {
        List<EdgeSimple> edgeList = new ArrayList<>();
        edgeList.add(new EdgeSimple(1,6));
        edgeList.add(new EdgeSimple(1,2));
        edgeList.add(new EdgeSimple(1,3));
        edgeList.add(new EdgeSimple(1,4));
        edgeList.add(new EdgeSimple(1,5));
        edgeList.add(new EdgeSimple(2,6));
        edgeList.add(new EdgeSimple(2,7));
        edgeList.add(new EdgeSimple(2,3));
        edgeList.add(new EdgeSimple(2,4));
        edgeList.add(new EdgeSimple(2,5));
        edgeList.add(new EdgeSimple(3,6));
        edgeList.add(new EdgeSimple(3,7));
        edgeList.add(new EdgeSimple(3,8));
        edgeList.add(new EdgeSimple(3,4));
        edgeList.add(new EdgeSimple(3,5));
        edgeList.add(new EdgeSimple(4,6));
        edgeList.add(new EdgeSimple(4,7));
        edgeList.add(new EdgeSimple(4,8));
        edgeList.add(new EdgeSimple(4,9));
        edgeList.add(new EdgeSimple(4,5));
        this.edges = edgeList;
    }

    public void generateGraphTenRandomRemainder(int numbEdges, int remainder) {

        List<EdgeSimple> edgeList = new ArrayList<>();
        for (int i = 0; i < numbEdges; i++) {
            edgeList.add(new EdgeSimple(1,i%remainder + 2));
        }
        this.edges = edgeList;
    }

    public void generateRandomGraph(int numbEdges) {
        List<EdgeSimple> edgeList = new ArrayList<>();
        Random rand = new Random();
        for (int i = 0; i < numbEdges; i++) {
            edgeList.add(new EdgeSimple(rand.nextInt(numbEdges+1),numbEdges+1));
        }
        this.edges = edgeList;
    }

    public void printGraph() {
        System.out.println("This is the graph with its edges: ");
        for (EdgeSimple e: this.edges) {
            System.out.print(e.getOriginVertex() + "," + e.getDestinVertex() + " || ");
        }
        System.out.println();
        System.out.println("Total number of edges: " + this.edges.size());
        System.out.println("------------------");
    }

    public List<EdgeEvent> getSyntheticGraphWithEvents() {
        return edgeEvents;
    }

    public DataStream<EdgeEvent> getEdgeStream (StreamExecutionEnvironment env) {

        DataStream<EdgeEvent> edgeEventStream = env.fromCollection(this.edgeEvents)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<EdgeEvent>() {
                    @Override
                    public long extractAscendingTimestamp(EdgeEvent element) {
                        return element.getEventTime();
                    }
                });

        return edgeEventStream;
    }

}




