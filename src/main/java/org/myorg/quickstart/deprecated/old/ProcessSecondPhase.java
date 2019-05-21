package org.myorg.quickstart.deprecated.old;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.deprecated.EdgeEventDepr;

import java.util.*;


public class ProcessSecondPhase extends ProcessWindowFunction<EdgeEventDepr, HashMap, Integer, TimeWindow> {

    int counter = 0;
    HashMap<Integer, Set<Integer>> stateTable = new HashMap<>();

    public void process(Integer key, Context context, Iterable<EdgeEventDepr> edgeIterable, Collector<HashMap> out) throws Exception {


        // Store all edges of current window
        List<EdgeEventDepr> edgesInWindow = storeElementsOfWindow(edgeIterable);
        printWindowElements(edgesInWindow);

        for(EdgeEventDepr e: edgesInWindow) {
            findPartition(e);
        }

        out.collect(stateTable);

    }

    public List<EdgeEventDepr> storeElementsOfWindow(Iterable<EdgeEventDepr> edgeIterable) {

        // Save into List
        List<EdgeEventDepr> edgesInWindow = new ArrayList<>();
        edgeIterable.forEach(edgesInWindow::add);

        return edgesInWindow;
    }

    public void printWindowElements(List<EdgeEventDepr> edgeEventDeprList) {

        // Create human-readable String with current window
        String printString = "1st Phase Window [" + counter++ + "]: ";
        for(EdgeEventDepr e: edgeEventDeprList) {
            printString = printString + "; " + e.getEdge().getOriginVertex() + " " + e.getEdge().getDestinVertex();
        }
        // Optionally: Print
        System.out.println(printString);

    }

    public void findPartition(EdgeEventDepr e) {
        Integer[] vertices = new Integer[2];
        vertices[0] = e.getEdge().getOriginVertex();
        vertices[1] = e.getEdge().getDestinVertex();
        Long highest = 0L;

        // Delete if "tag" is used
        int mostFreq = vertices[0];

        // Tagging for partitions

        Long count;

        int partitionId = choosePartition(e);
        // Loop over both vertices and see which one has the higher degree (if equal, the left vertex "wins").
        for (int i = 0; i < 2; i++) {
            Set<Integer> partitionSet = new HashSet<>();
            if (stateTable.containsKey(vertices[i])) {
                String partitionsString = vertices[i] + " in: ";
                for(Integer partition: stateTable.get(vertices[0])) {
                    partitionSet.add(partition);
                    partitionsString = partitionsString + ", ";
                }
                partitionSet.add(partitionId);
                stateTable.put(vertices[i], partitionSet);
            } else {
                partitionSet.add(partitionId);
                stateTable.put(vertices[i], partitionSet);
            }
            /*if (count > highest) {
                highest = count;
                mostFreq = Integer.parseInt(tuple.getField(i));;
                //tag = (int) tuple.getField(i) % partitions;
            }*/
        }


    }

    public int choosePartition(EdgeEventDepr e) {
        Random rand = new Random();
        int selectedPartition = rand.nextInt(4);
        return selectedPartition;
    }
}