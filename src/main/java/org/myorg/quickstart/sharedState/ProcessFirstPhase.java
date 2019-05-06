package org.myorg.quickstart.sharedState;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;


public class ProcessFirstPhase extends ProcessWindowFunction<EdgeEvent, HashMap, Integer, TimeWindow> {

    int counter = 0;
    HashMap<Integer, Integer> frequencyTable = new HashMap<>();

    public void process(Integer key, Context context, Iterable<EdgeEvent> edgeIterable, Collector<HashMap> out) throws Exception {

        // Store all edges of current window
        List<EdgeEvent> edgesInWindow = storeElementsOfWindow(edgeIterable);
        printWindowElements(edgesInWindow);

        for(EdgeEvent e: edgesInWindow) {
            getFrequency(e);
        }

        out.collect(frequencyTable);

    }

    public List<EdgeEvent> storeElementsOfWindow(Iterable<EdgeEvent> edgeIterable) {

        // Save into List
        List<EdgeEvent> edgesInWindow = new ArrayList<>();
        edgeIterable.forEach(edgesInWindow::add);

        return edgesInWindow;
    }

    public void printWindowElements(List<EdgeEvent> edgeEventList) {

        // Create human-readable String with current window
        String printString = "1st Phase Window [" + counter++ + "]: ";
        for(EdgeEvent e: edgeEventList) {
            printString = printString + "; " + e.getEdge().getOriginVertex() + " " + e.getEdge().getDestinVertex();
        }
        // Optionally: Print
        //System.out.println(printString);

    }

    public void getFrequency(EdgeEvent e) {
        Integer[] vertices = new Integer[2];
        vertices[0] = e.getEdge().getOriginVertex();
        vertices[1] = e.getEdge().getDestinVertex();
        Long highest = 0L;

        // Delete if "tag" is used
        int mostFreq = vertices[0];

        // Tagging for partitions

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