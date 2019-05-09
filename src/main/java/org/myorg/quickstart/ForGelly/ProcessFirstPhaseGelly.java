package org.myorg.quickstart.ForGelly;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.sharedState.EdgeEvent;
import org.myorg.quickstart.sharedState.PhasePartitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static java.time.Instant.now;


public class ProcessFirstPhaseGelly extends ProcessWindowFunction<EdgeEventGelly, HashMap, Integer, TimeWindow> {

    private String algorithm;
    int windowCounter = 0;
    int edgeCounter = 0;

    public ProcessFirstPhaseGelly(String algorithm) {
        this.algorithm = algorithm;
    }

    public void process(Integer key, Context context, Iterable<EdgeEventGelly> edgeIterable, Collector<HashMap> out) throws Exception {

        windowCounter++;
        int edgeCounter2 = 0;


        String printString = " - ";
        HashMap<Integer, HashSet<Integer>> vertexToPartitionMap = new HashMap<>();

        // Store all edges of current window
        List<EdgeEventGelly> edgesInWindow = storeElementsOfWindow(edgeIterable);
        printWindowElements(edgesInWindow);

        // For every edge, choose partition based on algorithm
        ModelBuilderGelly modelBuilder = new ModelBuilderGelly(algorithm, vertexToPartitionMap);

        for(EdgeEventGelly e: edgesInWindow) {
            modelBuilder.choosePartition(e);
            printString = printString + e.getEdge().f0 + " " + e.getEdge().f1 + ", ";
        }

        if (PhasePartitioner.printPhaseOne == true) {
            printString = now() + "P1: window # " + windowCounter + " -- edges: " + edgesInWindow.size() + printString + " --(Model)";
            //System.out.println(printString);
        }
        // Emit local model for next phase
        out.collect(modelBuilder.getVertexToPartitionMap());

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

}