package org.myorg.quickstart.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.types.NullValue;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class GraphCreatorGelly {

/*    private List<EdgeSimple> edges;
    private List<EdgeEventDepr> edgeEvents;*/

    private List<Edge> edges;
    private List<EdgeEventGelly> edgeEvents;
    private StreamExecutionEnvironment env;

    public GraphCreatorGelly(String characteristic, int graphSize, StreamExecutionEnvironment env) throws Exception {

        this.env = env;

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

        generateEdgeEvents();

    }

    public GraphCreatorGelly(String characteristic, String inputPath, StreamExecutionEnvironment env) throws Exception {
        if (!characteristic.equals("file")) {
            throw new Exception("WRONG GRAPH TYPE");
        }
        this.env = env;
        readGraphFromFile(inputPath);
        generateEdgeEvents();
    }

    public void generateEdgeEvents() throws InterruptedException {
        ArrayList<EdgeEventGelly> edgeEventList = new ArrayList<>();
        for (Edge e: this.getEdges()) {
//            System.out.println(e);
            edgeEventList.add(new EdgeEventGelly(e));
            Thread.sleep(TEMPGLOBALVARIABLES.sleep);
        }
        this.edgeEvents = edgeEventList;
    }

    public void readGraphFromFile(String inputPath) throws IOException {
        List<Edge> edgeList = new ArrayList<>();

        String line = null;
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader =
                    new FileReader(inputPath);
            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                String[] lineArray = line.split(",");
                long src = Long.parseLong(lineArray[0]);
                long trg = Long.parseLong(lineArray[1]);
                edgeList.add(new Edge<>(src,trg,NullValue.getInstance()));
            }
            // Always close files.
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" +
                            inputPath + "'");
        }
        this.edges = edgeList;

    }

    // empty graph -- needs to call a "generate" functions after
    public GraphCreatorGelly() {
        }

    public List<Edge> getEdges() {
        return edges;
    }

    public List<EdgeEventGelly> getEdgeEvents() {
        return edgeEvents;
    }

    public void printEdgeEventsWithTimestamp() {
        for (EdgeEventGelly e: this.edgeEvents) {
            System.out.println(e.getEdge().f0 + " " + e.getEdge().f1 + " "
                    + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(e.getEventTime())));
        }
    }

    public void generateGraphOneToAny(int numbEdges) throws InterruptedException {
        List<Edge> edgeList = new ArrayList<>();
        for (long i = 1; i < numbEdges+1; i++) {
            edgeList.add(new Edge<>(i, i+1, null));
        }
        this.edges = edgeList;
    }

    public void generateGraphOneTwoToAny(int numbEdges) {
        List<Edge> edgeList = new ArrayList<>();
        for (long i = 1; i < numbEdges/2+1; i++) {
            edgeList.add(new Edge<>(1L,i+1,null));
        }
        for (long i = numbEdges/2+1; i < numbEdges+1; i++) {
            edgeList.add(new Edge<>(2L,i+1,null));
        }
        this.edges = edgeList;
    }


    public void generateGraphModulo10(int numbEdges) throws Exception {
        if (numbEdges%10 != 0) {
            throw new Exception("Provide a graphSize divisible by 10");
        }
        List<Edge> edgeList = new ArrayList<>();
        long originEdge = 1;
        for (long i = 1; i < numbEdges; i++) {
            if (i %10 == 0)
                originEdge++;
            edgeList.add(new Edge<>(originEdge,originEdge + i,null));
        }
        this.edges = edgeList;
    }

    public void generateGraphForOriginPartitioning(int numbEdges) throws Exception {
        List<Edge> edgeList = new ArrayList<>();
        for (long i = 1; i < numbEdges; i++) {
            edgeList.add(new Edge<>(1L,i+1,null));
            edgeList.add(new Edge<>(2L,i+1,null));
            edgeList.add(new Edge<>(3L,i+1,null));
            edgeList.add(new Edge<>(4L,i+1,null));
        }
        this.edges = edgeList;
    }


    public void generateGraphOneFiveToFive() {
        List<Edge> edgeList = new ArrayList<>();
        edgeList.add(new Edge<>(1L,6L,null));
        edgeList.add(new Edge<>(1L,2L,null));
        edgeList.add(new Edge<>(1L,3L,null));
        edgeList.add(new Edge<>(1L,4L,null));
        edgeList.add(new Edge<>(1L,5L,null));
        edgeList.add(new Edge<>(2L,6L,null));
        edgeList.add(new Edge<>(2L,7L,null));
        edgeList.add(new Edge<>(2L,3L,null));
        edgeList.add(new Edge<>(2L,4L,null));
        edgeList.add(new Edge<>(2L,5L,null));
        edgeList.add(new Edge<>(3L,6L,null));
        edgeList.add(new Edge<>(3L,7L,null));
        edgeList.add(new Edge<>(3L,8L,null));
        edgeList.add(new Edge<>(3L,4L,null));
        edgeList.add(new Edge<>(3L,5L,null));
        edgeList.add(new Edge<>(4L,6L,null));
        edgeList.add(new Edge<>(4L,7L,null));
        edgeList.add(new Edge<>(4L,8L,null));
        edgeList.add(new Edge<>(4L,9L,null));
        edgeList.add(new Edge<>(4L,5L,null));
        this.edges = edgeList;
    }

    public void generateGraphTenRandomRemainder(int numbEdges, int remainder) {

        List<Edge> edgeList = new ArrayList<>();
        for (int i = 0; i < numbEdges; i++) {
            edgeList.add(new Edge<>(1,i%remainder + 2,null));
        }
        this.edges = edgeList;
    }

    public void generateRandomGraph(int numbEdges) {
        List<Edge> edgeList = new ArrayList<>();
        Random rand = new Random();
        for (int i = 0; i < numbEdges; i++) {
            edgeList.add(new Edge<>(rand.nextInt(numbEdges+1),numbEdges+1,null));
        }
        this.edges = edgeList;
    }

    public void printGraph() {
        System.out.println("This is the graph with its edges: ");
        for (Edge e: this.edges) {
            System.out.print(e.f0 + "," + e.f1 + " || ");
        }
        System.out.println();
        System.out.println("Total number of edges: " + this.edges.size());
        System.out.println("------------------");
    }

    public List<EdgeEventGelly> getSyntheticGraphWithEvents() {
        return edgeEvents;
    }

    public DataStream<EdgeEventGelly> getEdgeStream (StreamExecutionEnvironment env) {

        DataStream<EdgeEventGelly> edgeEventStream = env.fromCollection(this.edgeEvents)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<EdgeEventGelly>() {
                    @Override
                    public long extractAscendingTimestamp(EdgeEventGelly element) {
                        return element.getEventTime();
                    }
                });

        return edgeEventStream;
    }

    public DataStream<EdgeEventGelly> getStreamFromFile(String inputPath) throws IOException {


        return env.readTextFile(inputPath)
                .map(new MapFunction<String, EdgeEventGelly>() {
                    @Override
                    public EdgeEventGelly map(String s) throws Exception {
                        String[] fields = s.split("\\,");
                        long src = Long.parseLong(fields[0]);
                        long trg = Long.parseLong(fields[1]);
                        return new EdgeEventGelly(new Edge<>(src, trg, NullValue.getInstance()));
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<EdgeEventGelly>() {
                    @Override
                    public long extractAscendingTimestamp(EdgeEventGelly element) {
                        return element.getEventTime();
                    }
                });

    }

    public DataStream<EdgeEventGelly> getGraphStream(String inputPath) throws IOException {

        return env.readTextFile(inputPath)
                .map(new MapFunction<String, EdgeEventGelly>() {
                    @Override
                    public EdgeEventGelly map(String s) throws Exception {
                        String[] fields = s.split("\\,");
                        long src = Long.parseLong(fields[0]);
                        long trg = Long.parseLong(fields[1]);
                        return new EdgeEventGelly(new Edge<>(src, trg, NullValue.getInstance()));
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<EdgeEventGelly>() {
            @Override
            public long extractAscendingTimestamp(EdgeEventGelly element) {
                return element.getEventTime();
            }
        });

    }

    public DataStream<EdgeEventGelly> getGraphFromFile(String inputPath) throws IOException, InterruptedException {

        // This will reference one line at a time
        String line = null;
        List<Edge> edges = new ArrayList<>();
        List<EdgeEventGelly> edgeEvents = new ArrayList<>();
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader =
                    new FileReader(inputPath);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                String[] lineArray = line.split(",");
                long src = Long.parseLong(lineArray[0]);
                long trg = Long.parseLong(lineArray[1]);
                Edge e = new Edge<>(src, trg, NullValue.getInstance());
                edges.add(e);
                edgeEvents.add(new EdgeEventGelly(e));
                Thread.sleep(TEMPGLOBALVARIABLES.sleep);

            }

            // Always close files.
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" +
                            inputPath + "'");
        }

        this.edges = edges;
        this.edgeEvents = edgeEvents;

        DataStream<EdgeEventGelly> edgeEventStream = env.fromCollection(edgeEvents)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<EdgeEventGelly>() {
                    @Override
                    public long extractAscendingTimestamp(EdgeEventGelly element) {
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




