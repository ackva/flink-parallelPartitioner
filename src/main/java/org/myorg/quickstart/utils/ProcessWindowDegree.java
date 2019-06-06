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


public class ProcessWindowDegree extends ProcessWindowFunction<Edge<Integer, NullValue>, HashMap<Integer, Integer>, Integer, TimeWindow> {

    private String algorithm;
    int windowCounter = 0;
    int edgeCounter = 0;
    public ProcessWindowDegree() {

    }

    // The process function keeps a hashmap that tracks the vertex degrees ** per Window AND key **
    public void process(Integer key, Context context, Iterable<Edge<Integer, NullValue>> edgeIterable, Collector<HashMap<Integer, Integer>> out) throws Exception {

        windowCounter++;

        HashMap<Integer, Integer> vertexDegreeMap = new HashMap<>();

        // Store all edges of current window
        List<Edge> edgesInWindow = storeElementsOfWindow(edgeIterable);

        // Maintain degree HashMap (Key: vertex || Value: degree)
        for(Edge e: edgesInWindow) {

            int source = Integer.parseInt(e.f0.toString());
            int target = Integer.parseInt(e.f1.toString());
            // Add source vertex with degree 1, if no map entry exists. Otherwise, increment by 1
            if (!vertexDegreeMap.containsKey(source))
                vertexDegreeMap.put(source, 1);
            else
                vertexDegreeMap.put(source, vertexDegreeMap.get(source) + 1);

            // Add target vertex with degree 1, if no map entry exists. Otherwise, increment by 1
            if (!vertexDegreeMap.containsKey(target))
                vertexDegreeMap.put(target, 1);
            else
                vertexDegreeMap.put(target, vertexDegreeMap.get(target) + 1);
        }

        // Print operations for debugging
        if (TEMPGLOBALVARIABLES.printPhaseOne) {
            edgeCounter = edgeCounter + edgesInWindow.size();
            if (windowCounter % 100 == 0) {
                String edgesInWindowString = "";
                if (edgesInWindow.size() < 4)
                    edgesInWindowString = " --> " + printWindowElements(edgesInWindow);
                System.out.println("P1 - degre: window # " + windowCounter + " . nr of elements: " + edgesInWindow.size() + edgesInWindowString);
            }
        }
        // Emit local model for next phase
        out.collect(vertexDegreeMap);

    }

    public List<Edge> storeElementsOfWindow(Iterable<Edge<Integer, NullValue>> edgeIterable) {

        // Save into List
        List<Edge> edgesInWindow = new ArrayList<>();
        edgeIterable.forEach(edgesInWindow::add);

        return edgesInWindow;
    }

    public String printWindowElements(List<Edge> edgeList) {

        // Create human-readable String with current window
        String printString = "";
        for(Edge e: edgeList) {
            printString = printString + "; " + e.f0 + " " + e.f1;
        }

        return printString;

    }

}