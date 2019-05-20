package org.myorg.quickstart.deprecated.old;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.deprecated.EdgeEvent;
import org.myorg.quickstart.deprecated.EdgeSimple;
import org.myorg.quickstart.deprecated.GraphCreator;

import java.util.*;

public class BroadcastPartitionerWindowed {

    final static Class<Tuple2<Integer, ArrayList<Integer>>> typedTuple = (Class<Tuple2<Integer, ArrayList<Integer>>>) (Class<?>) Tuple2.class;

    final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(
            typedTuple,
            new GenericTypeInfo<>(Integer.class),
            new GenericTypeInfo<>(Integer.class)
    );

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {

        // Argument fetching
        int graphSize = 100;

        // Environment setup
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // MapState Descriptor (as from data artisans)
        MapStateDescriptor<String, Tuple2<Integer, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                tupleTypeInfo
        );


/*
        // SHOULD BE TEMPORARY
        // #### Create 1 sample "state" for Vertex 1, appearing in partition 1
        List<Integer> stateArray = new ArrayList<>();
        stateArray.add(-1);
        stateArray.add(-1);
        List<Tuple2<Integer, List<Integer>>> stateList = new ArrayList<>();
        stateList.add(new Tuple2<>(new Integer(-1), stateArray));

        BroadcastStream<Tuple2<Integer,List<Integer>>> broadcastRulesStream = env.fromCollection(stateList)
                //BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream2 = outputRules
                .flatMap(new FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>>() {
                    @Override
                    public void flatMap(Tuple2<Integer, List<Integer>> value, Collector<Tuple2<Integer, List<Integer>>> out) {
                        out.collect(value);
                    }
                })
                .setParallelism(2)
                .broadcast(rulesStateDescriptor);
        // END -- SHOULD BE TEMPORARY
*/


        // ### Generate graph and make "fake events" (for window processing)
        // Generate a graph
        System.out.println("Number of edges: " + graphSize);
        GraphCreator tgraph = new GraphCreator();
        tgraph.generateGraphOneTwoToAny(graphSize);
        List<EdgeSimple> edgeList = tgraph.getEdges();
        // Assign event time (=now) for every edge and printPhaseOne this list
        List<EdgeEvent> edgeEvents = new ArrayList<>();
        for (int i = 0; i < graphSize; i++) {
            edgeEvents.add(new EdgeEvent(edgeList.get(i)));
            //System.out.println(edgeEvents.get(i).getEdge().getOriginVertex() + " "
            //       + edgeEvents.get(i).getEdge().getDestinVertex() + " -- Time: " + edgeEvents.get(i).getEventTime());
        }

        // ### Create Edge Stream from input graph
        // Assign timestamps to the stream
        DataStream<EdgeEvent> edgeEventStream = env.fromCollection(edgeEvents)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<EdgeEvent>() {
                    @Override
                    public long extractAscendingTimestamp(EdgeEvent element) {
                        return element.getEventTime();
                    }
                });

        // Window Edge Stream, by time
        WindowedStream windowedEdgeStream = edgeEventStream
                .keyBy(new KeySelector<EdgeEvent, Integer>() {
                    @Override
                    public Integer getKey(EdgeEvent value) throws Exception {
                        return value.getEdge().getOriginVertex();
                    }
                })
                .timeWindow(Time.milliseconds(1));

        // Process all Windows into a "new" DataStream
        DataStream<Tuple2<List<EdgeEvent>, Integer>> processedWindowedEdges = windowedEdgeStream
                //.trigger(CountTrigger.of(5))
                .process(new ProcessEdgeWindow() {
                //.process(new ProcessEdgeWindow(broadcastRulesStream, env) {
                });

        KeyedStream<EdgeSimple, Integer> edgeStreamForPartitioning = processedWindowedEdges
                .flatMap(new FlatMapFunction<Tuple2<List<EdgeEvent>, Integer>, EdgeSimple>() {
                    @Override
                    public void flatMap(Tuple2<List<EdgeEvent>, Integer> value, Collector< EdgeSimple> out) throws Exception {
                        for (int i = 0; i < value.f0.size(); i++) {
                            out.collect(value.f0.get(i).getEdge());
                        }
                    }
                })
                //.rebalance()    // needed to increase the parallelism
                //.map(edgeSimple -> edgeSimple)
                .setParallelism(2)
                .keyBy(EdgeSimple::getOriginVertex);



        //processedWindowedEdges.printPhaseOne();


/*
        // Match Function to connect broadcast (state) and edges
        MatchFunctionEdgeEvents matchRules = new MatchFunctionEdgeEvents();
        matchRules.setRound(1);

        DataStream<Tuple2<Integer, List<Integer>>> outputRules = edgeStreamForPartitioning
                .connect(broadcastRulesStream)
                .process(matchRules);*/

        //outputRules.printPhaseOne();



/*
        // "Testing" function to see how window processing behaves
        DataStream<Tuple2<EdgeEvent, Integer>> test123 = processedWindowedEdges
                .flatMap(new FlatMapFunction<Tuple2<List<EdgeEvent>, Integer>, Tuple2<EdgeEvent, Integer>>() {
                    @Override
                    public void flatMap(Tuple2<List<EdgeEvent>, Integer> value, Collector< Tuple2<EdgeEvent, Integer>> out) throws Exception {
                        for (int i = 0; i < value.f0.size(); i++) {
                            out.collect(new Tuple2<>(value.f0.get(i), value.f1));
                        }
                    }
                });
        test123.printPhaseOne();*/



        // ### Finally, execute the job in Flink
        env.execute();

    } // close main method

    public static class ProcessEdgeWindow extends ProcessWindowFunction<EdgeEvent, Tuple2<List<EdgeEvent>, Integer>, Integer, TimeWindow> {

        //    private static ClassLoader

        final static Class<Tuple2<Integer, ArrayList<Integer>>> typedTuple = (Class<Tuple2<Integer, ArrayList<Integer>>>) (Class<?>) Tuple2.class;

        final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(
                typedTuple,
                new GenericTypeInfo<>(Integer.class),
                new GenericTypeInfo<>(Integer.class)
        );

        List<Tuple2<Integer, List<Integer>>> stateList = new ArrayList<>();
        List<Integer> stateArray = new ArrayList<>();
        List<EdgeEvent> edgesInWindow = new ArrayList<>();
        BroadcastStream<Tuple2<Integer,List<Integer>>> broadcastStream;
        StreamExecutionEnvironment env;

        //HashMap placedEdges = new HashMap();
        //HashMap vertexTable = new HashMap();



        public void process(Integer key, Context context, Iterable<EdgeEvent> edgeIterable, Collector<Tuple2<List<EdgeEvent>, Integer>> out) throws Exception {
//      int counterBroadcast = 0;

            // Print current Window (edges)
            edgeIterable.forEach(edgesInWindow::add);
            String printString = "Current Window: ";
            List<EdgeSimple> fakeList = new ArrayList<>();
            for(EdgeEvent e: edgesInWindow) {
                printString = printString + "; " + e.getEdge().getOriginVertex() + " " + e.getEdge().getDestinVertex();
                fakeList.add(e.getEdge());
            }



            // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            // MapState Descriptor (as from data artisans)
            MapStateDescriptor<String, Tuple2<Integer, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>(
                    "RulesBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    tupleTypeInfo
            );

            stateArray.add(-1);
            stateArray.add(-1);
            stateList.add(new Tuple2<>(new Integer(-1), stateArray));

            // SHOULD BE TEMPORARY
            BroadcastStream<Tuple2<Integer,List<Integer>>> broadcastRulesStream = env.fromCollection(stateList)
                    //BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream2 = outputRules
                    .flatMap(new FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>>() {
                        @Override
                        public void flatMap(Tuple2<Integer, List<Integer>> value, Collector<Tuple2<Integer, List<Integer>>> out) {
                            out.collect(value);
                        }
                    })
                    .setParallelism(2)
                    .broadcast(rulesStateDescriptor);
            // END -- SHOULD BE TEMPORARY

            DataStream<EdgeSimple> edgeStreamForPartitioning = env.fromCollection(fakeList)
                    .map(edgeSimple -> edgeSimple)
                    .keyBy(EdgeSimple::getDestinVertex);

            // Match Function to connect broadcast (state) and edges
            MatchFunctionEdgeEvents matchRules2 = new MatchFunctionEdgeEvents();
            matchRules2.setRound(2);

            DataStream<Tuple2<Integer, List<Integer>>> outputRules = edgeStreamForPartitioning
                    .connect(broadcastRulesStream)
                    .process(matchRules2);


            env.execute();
            System.out.println(printString);

            //System.out.println("TEST TEST");

        }
    }




}
