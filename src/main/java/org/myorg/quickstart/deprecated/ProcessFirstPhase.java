package org.myorg.quickstart.deprecated;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.utils.TEMPGLOBALVARIABLES;

import java.util.*;

import static java.time.Instant.now;


public class ProcessFirstPhase extends ProcessWindowFunction<EdgeEventDepr, HashMap, Integer, TimeWindow> {

    int windowCounter = 0;
    int edgeCounter = 0;

    public void process(Integer key, Context context, Iterable<EdgeEventDepr> edgeIterable, Collector<HashMap> out) throws Exception {

        windowCounter++;
        int edgeCounter2 = 0;


        String printString = " - ";
        HashMap<Integer, HashSet<Integer>> vertexToPartitionMap = new HashMap<>();

        // Store all edges of current window
        List<EdgeEventDepr> edgesInWindow = storeElementsOfWindow(edgeIterable);
        //printWindowElements(edgesInWindow);

        // For every edge, choose partition based on algorithm
        ModelBuilderDepr modelBuilderDepr = new ModelBuilderDepr("byOrigin", vertexToPartitionMap);

        for(EdgeEventDepr e: edgesInWindow) {
            modelBuilderDepr.choosePartition(e);
            printString = printString + e.getEdge().getOriginVertex() + " " + e.getEdge().getDestinVertex() + ", ";
        }

        if (TEMPGLOBALVARIABLES.printPhaseOne == true) {
            printString = now() + "P1: window # " + windowCounter + " -- edges: " + edgesInWindow.size() + printString + " --(Model)";
            //System.out.println(printString);
        }
        // Emit local model for next phase
        out.collect(modelBuilderDepr.getVertexToPartitionMap());

        // Print for debugging
        //System.out.println(windowCounter++ + " windows on this worker -- " + edgeCounter + " edges ");

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