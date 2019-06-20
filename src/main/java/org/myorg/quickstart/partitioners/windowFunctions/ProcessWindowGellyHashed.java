package org.myorg.quickstart.partitioners.windowFunctions;

import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.utils.TEMPGLOBALVARIABLES;

import java.util.ArrayList;
import java.util.List;


public class ProcessWindowGellyHashed extends ProcessWindowFunction<Edge<Integer, Long>, Edge<Integer, Long>, Integer, TimeWindow> {

    private int windowCounter = 0;
    private int avgEdgesPerWindow = 0;
    private int edgeCounter = 0;
    long currentWatermark = 1;
    List<Long> watermarks = new ArrayList<>();


    public void process(Integer key, Context context, Iterable<Edge<Integer, Long>> edgeIterable, Collector<Edge<Integer, Long>> out) throws Exception {


        if (currentWatermark != context.currentWatermark()) {
            watermarks.add(context.currentWatermark());
            currentWatermark = context.currentWatermark();
            System.out.println(" ELE _ new Watermark = " + currentWatermark + " old: " + watermarks);

        }

        //System.out.println("2$" + context.currentWatermark() + "$" + context.currentProcessingTime() + "$");

        // Temporary variables
        String printString = " - ";
        windowCounter++;

        //System.out.println("new window (" + windowCounter + ") " + context.currentProcessingTime() + " current watermark: " + context.currentWatermark());


        // Store all edges of current window
        List<Edge<Integer, Long>> edgesInWindow = storeElementsOfWindow(edgeIterable);
        //printWindowElements(edgesInWindow);

        for(Edge<Integer, Long> e: edgesInWindow) {
            //System.out.println(context.currentWatermark() + ": " + e);
            out.collect(e);
            //printString = printString + e.f0 + " " + e.f0 + ", ";
        }

        if (TEMPGLOBALVARIABLES.printPhaseOne) {
            edgeCounter = edgeCounter + edgesInWindow.size();
            avgEdgesPerWindow =  edgeCounter / windowCounter;
            if (windowCounter % 100000 == 0) {
                System.out.println("P1 - edges: window # " + windowCounter + " . nr of elements: " + edgesInWindow.size() +" avg = " + avgEdgesPerWindow);
            }
        }

/*        if (TEMPGLOBALVARIABLES.printTime && (windowCounter %500) == 0) {
            //ctx.output(GraphPartitionerImpl.outputTag, "REL > " + toBeRemoved.size() + " > " + globalCounterForPrint);
            System.out.println("WIN$" + windowCounter + "$" + context.currentWatermark());
        }*/

    }

    public String printWindowElements(List<Edge<Integer, Long>> edgeList) {

        // Create human-readable String with current window
        String printString = "";
        for(Edge e: edgeList) {
            printString = printString + "; " + e.f0 + " " + e.f1;
        }

        return printString;

    }

    public List<Edge<Integer, Long>> storeElementsOfWindow(Iterable<Edge<Integer, Long>> edgeIterable) {

        // Save into List
        List<Edge<Integer, Long>> edgesInWindow = new ArrayList<>();
        edgeIterable.forEach(edgesInWindow::add);

        return edgesInWindow;
    }

}