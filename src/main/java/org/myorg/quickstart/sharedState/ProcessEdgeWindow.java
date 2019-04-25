package org.myorg.quickstart.sharedState;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class ProcessEdgeWindow extends ProcessWindowFunction<EdgeEvent, Tuple2<List<EdgeEvent>, Integer>, Integer, TimeWindow> {

    public void process(Integer key, Context context, Iterable<EdgeEvent> edgeIterable, Collector<Tuple2<List<EdgeEvent>, Integer>> out) {
//            int counter = 0;
        List<EdgeEvent> edgesInWindow = new ArrayList<>();
        edgeIterable.forEach(edgesInWindow::add);
        String printString = "Current Window: ";
        for(EdgeEvent e: edgesInWindow) {
            printString = printString + "; " + e.getEdge().getOriginVertex() + " " + e.getEdge().getDestinVertex();
        }
        System.out.println(printString);
        out.collect(new Tuple2<>(edgesInWindow, edgesInWindow.size()));
    }
}