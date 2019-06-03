package org.myorg.quickstart.utils;

import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

import static java.time.Instant.now;


public class ProcessWindowGelly extends ProcessWindowFunction<Edge<Long, NullValue>, Edge<Long, NullValue>, Long, TimeWindow> {

    int windowCounter = 0;


    public void process(Long key, Context context, Iterable<Edge<Long, NullValue>> edgeIterable, Collector<Edge<Long, NullValue>> out) throws Exception {

        // Temporary variables
        String printString = " - ";
        windowCounter++;

        // Store all edges of current window
        List<Edge<Long, NullValue>> edgesInWindow = storeElementsOfWindow(edgeIterable);
        //printWindowElements(edgesInWindow);

        for(Edge<Long, NullValue> e: edgesInWindow) {
            out.collect(e);
            printString = printString + e.f0 + " " + e.f0 + ", ";
        }

        /*if (TEMPGLOBALVARIABLES.printPhaseOne == true) {
            printString = now() + "P1: window # " + windowCounter + " -- edges: " + edgesInWindow.size() + printString + " --(Edges)";
            System.out.println(printString);
        }*/

    }

    public List<Edge<Long, NullValue>> storeElementsOfWindow(Iterable<Edge<Long, NullValue>> edgeIterable) {

        // Save into List
        List<Edge<Long, NullValue>> edgesInWindow = new ArrayList<>();
        edgeIterable.forEach(edgesInWindow::add);

        return edgesInWindow;
    }

}