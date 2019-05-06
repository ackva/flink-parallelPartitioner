package org.myorg.quickstart.sharedState;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.mutable.HashTable;

import java.util.*;


public class ProcessSecondPhase extends ProcessWindowFunction<EdgeEvent, HashMap, Integer, TimeWindow> {

    int counter = 0;
    HashMap<Integer, Set<Integer>> stateTable = new HashMap<>();

    public void process(Integer key, Context context, Iterable<EdgeEvent> edgeIterable, Collector<HashMap> out) throws Exception {


        // Store all edges of current window
        List<EdgeEvent> edgesInWindow = storeElementsOfWindow(edgeIterable);
        printWindowElements(edgesInWindow);

        for(EdgeEvent e: edgesInWindow) {
            findPartition(e);
        }

        out.collect(stateTable);

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
        System.out.println(printString);

    }

    public void findPartition(EdgeEvent e) {
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

    public int choosePartition(EdgeEvent e) {
        Random rand = new Random();
        int selectedPartition = rand.nextInt(4);
        return selectedPartition;
    }
}