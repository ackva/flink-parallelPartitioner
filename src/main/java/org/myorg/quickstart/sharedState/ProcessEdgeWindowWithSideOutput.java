package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ProcessEdgeWindowWithSideOutput extends ProcessWindowFunction<EdgeEvent, Tuple2<DataStream<EdgeEvent>,Integer>, Integer, TimeWindow> {

    public List<Tuple2<Integer, List<Integer>>> stateList = new ArrayList<>();
    public List<Integer> stateArray = new ArrayList<>();
    int counter = 0;
    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ValueStateDescriptor<Integer> descriptor;
    private ValueState<Integer> state;
    final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

    // Constructor
    public ProcessEdgeWindowWithSideOutput() {
        stateArray.add(-1);
        stateArray.add(-1);
        stateList.add(new Tuple2<>(new Integer(-1), stateArray));


    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Integer> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Integer>() {}), // type information
                        Integer.valueOf(0)); // default value of the state, if nothing was set
        state = getRuntimeContext().getState(descriptor);
    }

    public void process(Integer key, Context context, Iterable<EdgeEvent> edgeIterable, Collector<Tuple2<DataStream<EdgeEvent>, Integer>> out) throws Exception {

        List<EdgeEvent> edgesInWindow = new ArrayList<>();

        edgeIterable.forEach(edgesInWindow::add);

        // Print current Window (edges)
        String printString = "Current Window: ";
        List<EdgeSimple> fakeList = new ArrayList<>();
        int newState = 0;
        for(EdgeEvent e: edgesInWindow) {
            printString = printString + "; " + e.getEdge().getOriginVertex() + " " + e.getEdge().getDestinVertex();
            fakeList.add(e.getEdge());
            newState = newState + e.getEdge().getDestinVertex();
        }

        int currentState = state.value();
        state.update(newState);

        DataStream<EdgeEvent> edgeEventSubStream = env.fromCollection(edgesInWindow);

        out.collect(new Tuple2<>(edgeEventSubStream, counter));

        context.output(outputTag, counter + ":" + edgesInWindow.size() + "-- stateBefore: " + currentState + "; newState: " + newState);
        //System.out.println(printString);

        edgesInWindow.clear();

    }
}