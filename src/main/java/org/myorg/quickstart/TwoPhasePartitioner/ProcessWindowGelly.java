package org.myorg.quickstart.TwoPhasePartitioner;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.sharedState.EdgeEvent;
import org.myorg.quickstart.sharedState.PhasePartitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.time.Instant.now;


public class ProcessWindowGelly extends ProcessWindowFunction<EdgeEventGelly, EdgeEventGelly, Integer, TimeWindow> {

    int windowCounter = 0;


    public void process(Integer key, Context context, Iterable<EdgeEventGelly> edgeIterable, Collector<EdgeEventGelly> out) throws Exception {

        // Temporary variables
        String printString = " - ";
        windowCounter++;

        // Store all edges of current window
        List<EdgeEventGelly> edgesInWindow = storeElementsOfWindow(edgeIterable);
        //printWindowElements(edgesInWindow);

        for(EdgeEventGelly e: edgesInWindow) {
            out.collect(e);
            printString = printString + e.getEdge().f0 + " " + e.getEdge().f0 + ", ";
        }

        /*if (PhasePartitionerGelly.printPhaseOne == true) {
            printString = now() + "P1: window # " + windowCounter + " -- edges: " + edgesInWindow.size() + printString + " --(Edges)";
            System.out.println(printString);
        }*/

    }

    public List<EdgeEventGelly> storeElementsOfWindow(Iterable<EdgeEventGelly> edgeIterable) {

        // Save into List
        List<EdgeEventGelly> edgesInWindow = new ArrayList<>();
        edgeIterable.forEach(edgesInWindow::add);

        return edgesInWindow;
    }

    public void printWindowElements(List<EdgeEvent> edgeEventList) {

        // Create human-readable String with current window
        String printString = "1st Phase Window [" + windowCounter++ + "]: ";
        for(EdgeEvent e: edgeEventList) {
            printString = printString + "; " + e.getEdge().getOriginVertex() + " " + e.getEdge().getDestinVertex();
        }
        System.out.println(printString);

    }

    public void buildLocalModel(EdgeEvent e, HashMap<Integer, Integer> vertexToPartitionMap) {
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

    public void getFrequency(EdgeEvent e, HashMap<Integer, Integer> frequencyTable) {
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