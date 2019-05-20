/*
package org.myorg.quickstart.deprecated.TwoPhasePartitioner;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.deprecated.EdgeEvent;
import org.myorg.quickstart.deprecated.PhasePartitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static java.time.Instant.now;


public class ProcessFirstPhaseGelly extends ProcessWindowFunction<EdgeEventGelly, HashMap, Integer, TimeWindow> {

    private String algorithm;
    int windowCounter = 0;
    int edgeCounter = 0;
    ModelBuilderGelly modelBuilder;

    public ProcessFirstPhaseGelly(String algorithm) {

        this.algorithm = algorithm;

    }

    public void process(Integer key, Context context, Iterable<EdgeEventGelly> edgeIterable, Collector<HashMap> out) throws Exception {

        windowCounter++;
        int edgeCounter2 = 0;


        String printString = " - ";

        HashMap<Long, Long> vertexDegreeMap = new HashMap<>();
        this.modelBuilder = new ModelBuilderGelly(algorithm, vertexDegreeMap, k, lambda);

        // Store all edges of current window
        List<EdgeEventGelly> edgesInWindow = storeElementsOfWindow(edgeIterable);

        for(EdgeEventGelly e: edgesInWindow) {
            modelBuilder.choosePartition(e);
            printString = printString + e.getEdge().f0 + " " + e.getEdge().f1 + ", ";
        }

        if (TEMPGLOBALVARIABLES.printPhaseOne) {
            printString = now() + "P1: window # " + windowCounter + " -- edges: " + edgesInWindow.size() + printString + " --(Model)";
            printWindowElements(edgesInWindow);
            System.out.println(printString);
        } else
            System.out.println(TEMPGLOBALVARIABLES.printPhaseOne);
        // Emit local model for next phase
        out.collect(modelBuilder.getVertexDegreeMap());

        // Print for debugging
        //System.out.println(windowCounter++ + " windows on this worker -- " + edgeCounter + " edges ");

    }

    public List<EdgeEventGelly> storeElementsOfWindow(Iterable<EdgeEventGelly> edgeIterable) {

        // Save into List
        List<EdgeEventGelly> edgesInWindow = new ArrayList<>();
        edgeIterable.forEach(edgesInWindow::add);

        return edgesInWindow;
    }

    public void printWindowElements(List<EdgeEventGelly> edgeEventList) {

        // Create human-readable String with current window
        String printString = "1st Phase Window [" + windowCounter++ + "]: ";
        for(EdgeEventGelly e: edgeEventList) {
            printString = printString + "; " + e.getEdge().f0 + " " + e.getEdge().f1;
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

}*/
