package org.myorg.quickstart.utils;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
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

        /*if (TEMPGLOBALVARIABLES.printPhaseOne == true) {
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

}