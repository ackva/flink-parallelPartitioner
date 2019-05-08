package org.myorg.quickstart.sharedState.old;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.myorg.quickstart.sharedState.EdgeEvent;
import org.myorg.quickstart.sharedState.EdgeSimple;
import org.myorg.quickstart.sharedState.TestingGraph;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Value state testing (of of "managed keyed states" in Flink)
 *
 */

public class ValueStateImplementation {

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {

        // Argument fetching
        int graphSize = 10000;

        // Environment setup
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

        // ### Generate graph and make "fake events" (for window processing)
        // Generate a graph
        System.out.println("Number of edges: " + graphSize);
        TestingGraph tgraph = new TestingGraph();
        tgraph.generateGraphOneTwoToAny(graphSize);
        List<EdgeSimple> edgeList = tgraph.getEdges();
        // Assign event time (=now) for every edge and printPhaseOne this list
        List<EdgeEvent> edgeEvents = new ArrayList<>();
        for (int i = 0; i < graphSize; i++)
            edgeEvents.add(new EdgeEvent(edgeList.get(i)));

        // ### Create Edge Stream from input graph
        // Assign timestamps to the stream
        KeyedStream keyedEdgeStream = env.fromCollection(edgeEvents)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<EdgeEvent>() {
                    @Override
                    public long extractAscendingTimestamp(EdgeEvent element) {
                        return element.getEventTime();
                    }
                })
                .keyBy(new KeySelector<EdgeEvent, Integer>() {
                    @Override
                    public Integer getKey(EdgeEvent value) throws Exception {
                        return value.getEdge().getOriginVertex();
                    }
                });

        SingleOutputStreamOperator windowedEdgeStream = keyedEdgeStream
                .timeWindow(Time.milliseconds(1))
                //.trigger(CountTrigger.of(5)) // --> used to show that behavior within same window is similar
                .process(new ProcessEdgeWindowWithSideOutput() {
                    //.process(new ProcessEdgeWindow(broadcastRulesStream, env) {
                });

        DataStream<String> sideOutputStream = windowedEdgeStream.getSideOutput(outputTag);

        // Print result
        sideOutputStream.print();

        // ### Finally, execute the job in Flink
        env.execute();
    }
}


class WindowProcessingWithValueState extends ProcessWindowFunction<EdgeEvent, Tuple2<DataStream<EdgeEvent>,Integer>, Integer, TimeWindow> {

    public List<Tuple2<Integer, List<Integer>>> stateList = new ArrayList<>();
    public List<Integer> stateArray = new ArrayList<>();
    int counter = 0;
    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ValueStateDescriptor<Integer> descriptor;
    private ValueState<Integer> state;
    final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

    // Constructor
    public WindowProcessingWithValueState() {
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

    /**
     * This function sums up all elements of one window (triggered) and adds this sum to the ValueState (state). A sideoutput (chosen for convenience) emits both: current and new state (sum)
     * @param key
     * @param context
     * @param edgeIterable
     * @param out
     * @throws Exception
     */
    public void process(Integer key, Context context, Iterable<EdgeEvent> edgeIterable, Collector<Tuple2<DataStream<EdgeEvent>, Integer>> out) throws Exception {

        counter++;
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

        context.output(outputTag, "-- stateBefore: " + currentState + "; newState: " + newState);
        //System.out.println(printString);

        edgesInWindow.clear();

    }
}
