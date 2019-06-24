package org.myorg.quickstart.partitioners.windowFunctions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class ProcessWindowDegreeWatermark extends ProcessWindowFunction<Edge<Integer, Long>, Tuple2<HashMap<Integer, Integer>,Long>, Integer, TimeWindow> {

    private String algorithm;
    int windowCounter = 0;
    int edgeCounter = 0;
    long startTime = System.currentTimeMillis();
    long currentWatermark = 1;
    List<Long> watermarks = new ArrayList<>();
    HashMap<Long, List<Edge>> edgesInWatermark = new HashMap<>();

    public ProcessWindowDegreeWatermark() {

    }

    // The process function keeps a hashmap that tracks the vertex degrees ** per Window AND key **
    public void process(Integer key, Context context, Iterable<Edge<Integer, Long>> edgeIterable, Collector<Tuple2<HashMap<Integer, Integer>,Long>> out) throws Exception {

        if (currentWatermark != context.currentWatermark()) {
            watermarks.add(context.currentWatermark());
            currentWatermark = context.currentWatermark();
            //System.out.println("DEG _ new Watermark = " + currentWatermark + " old: " + watermarks);
        }

        //System.out.println("new window (fake) " + context.currentProcessingTime() + " current watermark: " + context.currentWatermark());

        //System.out.println("1$" + context.currentWatermark() + "$" + context.currentProcessingTime() + "$");

        windowCounter++;
        edgeCounter++;

        HashMap<Integer, Integer> vertexDegreeMap = new HashMap<>();

        // Store all edges of current window
        List<Edge> edgesInWindow = storeElementsOfWindow(edgeIterable);

        // Maintain degree HashMap (Key: vertex || Value: degree)
        for(Edge e: edgesInWindow) {
            int source = Integer.parseInt(e.f0.toString());
            int target = Integer.parseInt(e.f1.toString());
            long edgeHash = Long.parseLong(e.f2.toString());
            //System.out.println("DEGRE > " + context.currentWatermark() + " > " + source + "|" + target + "|" + edgeHash);
            // Add source vertex with degree 1, if no map entry exists. Otherwise, increment by 1
            //float newHash = (source * target) % nextPrime;
            //hashString = hashString + newHash + ", ";
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

       // System.out.println("DEG: window hash value = " + windowHashValue + " with edges: " + edgesInWindow.size());

        //System.out.println(hashString);


        // Print operations for debugging
/*        if (TEMPGLOBALVARIABLES.printPhaseOne) {
            edgeCounter = edgeCounter + edgesInWindow.size();
            if (windowCounter % 100 == 0) {
                String edgesInWindowString = "";
                if (edgesInWindow.size() < 4)
                    edgesInWindowString = " --> " + printWindowElements(edgesInWindow);
                System.out.println("P1 - degre: window # " + windowCounter + " . nr of elements: " + edgesInWindow.size() + edgesInWindowString);
            }
        }*/


        // Emit local model for next phase
        out.collect(new Tuple2<>(vertexDegreeMap,0L));

/*
        if (TEMPGLOBALVARIABLES.printTime && (windowCounter %500) == 0) {
            //ctx.output(GraphPartitionerImpl.outputTag, "REL > " + toBeRemoved.size() + " > " + globalCounterForPrint);
            System.out.println("DEG$" + windowCounter + "$" + context.currentWatermark());
        }
*/

    }

    public List<Edge> storeElementsOfWindow(Iterable<Edge<Integer, Long>> edgeIterable) {

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