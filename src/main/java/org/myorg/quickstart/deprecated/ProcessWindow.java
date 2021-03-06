package org.myorg.quickstart.deprecated;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.utils.TEMPGLOBALVARIABLES;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.time.Instant.now;


public class ProcessWindow extends ProcessWindowFunction<EdgeEventDepr, EdgeEventDepr, Integer, TimeWindow> {

    int windowCounter = 0;


    public void process(Integer key, Context context, Iterable<EdgeEventDepr> edgeIterable, Collector<EdgeEventDepr> out) throws Exception {

        // Temporary variables
        String printString = " - ";
        windowCounter++;

        // Store all edges of current window
        List<EdgeEventDepr> edgesInWindow = storeElementsOfWindow(edgeIterable);
        //printWindowElements(edgesInWindow);

        for(EdgeEventDepr e: edgesInWindow) {
            out.collect(e);
            printString = printString + e.getEdge().getOriginVertex() + " " + e.getEdge().getDestinVertex() + ", ";
        }

        if (TEMPGLOBALVARIABLES.printPhaseOne == true) {
            printString = now() + "P1: window # " + windowCounter + " -- edges: " + edgesInWindow.size() + printString + " --(Edges)";
            System.out.println(printString);
        }

    }

    public List<EdgeEventDepr> storeElementsOfWindow(Iterable<EdgeEventDepr> edgeIterable) {

        // Save into List
        List<EdgeEventDepr> edgesInWindow = new ArrayList<>();
        edgeIterable.forEach(edgesInWindow::add);

        return edgesInWindow;
    }

    public void printWindowElements(List<EdgeEventDepr> edgeEventDeprList) {

        // Create human-readable String with current window
        String printString = "1st Phase Window [" + windowCounter++ + "]: ";
        for(EdgeEventDepr e: edgeEventDeprList) {
            printString = printString + "; " + e.getEdge().getOriginVertex() + " " + e.getEdge().getDestinVertex();
        }
        System.out.println(printString);

    }

    public void buildLocalModel(EdgeEventDepr e, HashMap<Integer, Integer> vertexToPartitionMap) {
        Integer[] vertices = new Integer[2];
        vertices[0] = e.getEdge().getOriginVertex();
        vertices[1] = e.getEdge().getDestinVertex();

        // Loop over both vertices and see which one has the higher degree (if equal, the left vertex "wins").
        for (int i = 0; i < 2; i++) {
            if (vertexToPartitionMap.containsKey(vertices[i])) {
                int currentCount = vertexToPartitionMap.get(vertices[i]) + 1;
                vertexToPartitionMap.put(vertices[i], currentCount);
            } else {
                vertexToPartitionMap.put(vertices[i], 1);
            }
        }
    }

    public void getFrequency(EdgeEventDepr e, HashMap<Integer, Integer> frequencyTable) {
        Integer[] vertices = new Integer[2];
        vertices[0] = e.getEdge().getOriginVertex();
        vertices[1] = e.getEdge().getDestinVertex();

        // Loop over both vertices and see which one has the higher degree (if equal, the left vertex "wins").
        for (int i = 0; i < 2; i++) {
            if (frequencyTable.containsKey(vertices[i])) {
                int currentCount = frequencyTable.get(vertices[i]) + 1;
                frequencyTable.put(vertices[i], currentCount);
            } else {
                frequencyTable.put(vertices[i], 1);
            }
        }


    }

}