package org.myorg.quickstart.deprecated;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class GraphCreator {

    private List<EdgeSimple> edges;
    private List<EdgeEventDepr> edgeEventDeprs;

    public GraphCreator(String characteristic, int graphSize) throws Exception {
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
            case "byOrigin":
                generateGraphForOriginPartitioning(graphSize);
                break;
            default:
                generateRandomGraph(graphSize);
                break;
        }

        ArrayList<EdgeEventDepr> edgeEventDeprList = new ArrayList<>();
        for (EdgeSimple e: this.getEdges()) {
//            System.out.println(e);
            edgeEventDeprList.add(new EdgeEventDepr(e));
            Thread.sleep(PhasePartitioner.sleep);
        }
        this.edgeEventDeprs = edgeEventDeprList;
    }

    // empty graph -- needs to call a "generate" functions after
    public GraphCreator() {
        }

    public List<EdgeSimple> getEdges() {
        return edges;
    }

    public List<EdgeEventDepr> getEdgeEventDeprs() {
        return edgeEventDeprs;
    }

    public void printEdgeEventsWithTimestamp() {
        for (EdgeEventDepr e: this.edgeEventDeprs) {
            System.out.println(e.getEdge().getOriginVertex() + " " + e.getEdge().getDestinVertex() + " "
                    + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(e.getEventTime())));
        }
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

    public void generateGraphForOriginPartitioning(int numbEdges) throws Exception {
        List<EdgeSimple> edgeList = new ArrayList<>();
        for (int i = 1; i < numbEdges; i++) {
            edgeList.add(new EdgeSimple(1,i+1));
            edgeList.add(new EdgeSimple(2,i+1));
            edgeList.add(new EdgeSimple(3,i+1));
            edgeList.add(new EdgeSimple(4,i+1));
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

    public List<EdgeEventDepr> getSyntheticGraphWithEvents() {
        return edgeEventDeprs;
    }

    public DataStream<EdgeEventDepr> getEdgeStream (StreamExecutionEnvironment env) {

        DataStream<EdgeEventDepr> edgeEventStream = env.fromCollection(this.edgeEventDeprs)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<EdgeEventDepr>() {
                    @Override
                    public long extractAscendingTimestamp(EdgeEventDepr element) {
                        return element.getEventTime();
                    }
                });

        return edgeEventStream;
    }

    // FROM ZAINAB'S CODE FOR HDRF
/*    public DataStream<EdgeDepr<Long, NullValue>> getGraphStream(StreamExecutionEnvironment env) throws IOException {

        return env.readTextFile(inputPath)
                .map(new MapFunction<String, EdgeDepr<Long, NullValue>>() {
                    @Override
                    public EdgeDepr<Long, NullValue> map(String s) throws Exception {
                        String[] fields = s.split("\\,");
                        long src = Long.parseLong(fields[0]);
                        long trg = Long.parseLong(fields[1]);
                        return new EdgeDepr<>(src, trg, NullValue.getInstance());
                    }
        });

    }*/

}




