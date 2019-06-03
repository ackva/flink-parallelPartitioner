package org.myorg.quickstart.utils;

import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.state.Graph;
import org.myorg.quickstart.deprecated.EdgeEventDepr;
import scala.xml.Null;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.time.Instant.now;


public class ProcessWindowDegree extends ProcessWindowFunction<Edge<Long, NullValue>, HashMap<Long, Long>, Long, TimeWindow> {

    private String algorithm;
    int windowCounter = 0;
    int edgeCounter = 0;

    public ProcessWindowDegree() {

    }

    // The process function keeps a hashmap that tracks the vertex degrees ** per Window AND key **
    public void process(Long key, Context context, Iterable<Edge<Long, NullValue>> edgeIterable, Collector<HashMap<Long, Long>> out) throws Exception {

        windowCounter++;

        HashMap<Long, Long> vertexDegreeMap = new HashMap<>();

        // Store all edges of current window
        List<Edge> edgesInWindow = storeElementsOfWindow(edgeIterable);

        // Maintain degree HashMap (Key: vertex || Value: degree)
        for(Edge e: edgesInWindow) {

            long source = Long.parseLong(e.f0.toString());
            long target = Long.parseLong(e.f1.toString());
            // Add source vertex with degree 1, if no map entry exists. Otherwise, increment by 1
            if (!vertexDegreeMap.containsKey(source))
                vertexDegreeMap.put(source, 1L);
            else
                vertexDegreeMap.put(source, vertexDegreeMap.get(source) + 1);

            // Add target vertex with degree 1, if no map entry exists. Otherwise, increment by 1
            if (!vertexDegreeMap.containsKey(target))
                vertexDegreeMap.put(target, 1L);
            else
                vertexDegreeMap.put(target, vertexDegreeMap.get(target) + 1);
        }

        // Print operations for debugging
        if (TEMPGLOBALVARIABLES.printPhaseOne) {
            String printString = " - ";
            printString = now() + "P1: window # " + windowCounter + " -- edges: " + edgesInWindow.size() + printString + " --(Model)";
            printWindowElements(edgesInWindow);
            //System.out.println(printString);
        }
        // Emit local model for next phase
        out.collect(vertexDegreeMap);

    }

    public List<Edge> storeElementsOfWindow(Iterable<Edge<Long, NullValue>> edgeIterable) {

        // Save into List
        List<Edge> edgesInWindow = new ArrayList<>();
        edgeIterable.forEach(edgesInWindow::add);

        return edgesInWindow;
    }

    public void printWindowElements(List<Edge> edgeEventList) {

        // Create human-readable String with current window

        String printString = "1st Phase Window [" + windowCounter++ + "]: ";
        for(Edge e: edgeEventList) {
            printString = printString + "; " + e.f0 + " " + e.f1;
        }
        if (TEMPGLOBALVARIABLES.printPhaseOne)
            System.out.println(printString);

    }

/*    public void buildLocalModel(EdgeEventDepr e, HashMap<Long, Long> vertexToPartitionMap) {
        long[] vertices = new long[2];
        vertices[0] = e.getEdge().getOriginVertex();
        vertices[1] = e.getEdge().getDestinVertex();

        // Loop over both vertices and see which one has the higher degree (if equal, the left vertex "wins").
        for (int i = 0; i < 2; i++) {
            if (vertexToPartitionMap.containsKey(vertices[i])) {
                long currentCount = vertexToPartitionMap.get(vertices[i]) + 1;
                vertexToPartitionMap.put(vertices[i], currentCount);
            } else {
                vertexToPartitionMap.put(vertices[i], 1L);
            }
        }
    }*/

    /*public void getFrequency(EdgeEventDepr e, HashMap<Long, Long> frequencyTable) {
        long[] vertices = new long[2];
        vertices[0] = e.getEdge().getOriginVertex();
        vertices[1] = e.getEdge().getDestinVertex();

        // Loop over both vertices and see which one has the higher degree (if equal, the left vertex "wins").
        for (int i = 0; i < 2; i++) {
            if (frequencyTable.containsKey(vertices[i])) {
                long currentCount = frequencyTable.get(vertices[i]) + 1;
                frequencyTable.put(vertices[i], currentCount);
            } else {
                frequencyTable.put(vertices[i], 1L);
            }
        }


    }*/

}