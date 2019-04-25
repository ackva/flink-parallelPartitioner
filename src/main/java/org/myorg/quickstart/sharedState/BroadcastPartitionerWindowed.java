package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.*;

public class BroadcastPartitionerWindowed {

    final static Class<Tuple2<Integer, ArrayList<Integer>>> typedTuple = (Class<Tuple2<Integer, ArrayList<Integer>>>) (Class<?>) Tuple2.class;

    final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(
            typedTuple,
            new GenericTypeInfo<>(Integer.class),
            new GenericTypeInfo<>(Integer.class)
    );

    public static void main(String[] args) throws Exception {

        // Argument fetching
        int graphSize = 50;

        // Environment setup
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // MapState Descriptor (as from data artisans)
        MapStateDescriptor<String, Tuple2<Integer, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                tupleTypeInfo
        );

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



        // ### Generate graph and make "fake events" (for window processing)
        // Generate a graph
        TestingGraph tgraph = new TestingGraph();
        tgraph.generateGraphOneTwoToAny(graphSize);
        List<EdgeSimple> edgeList = tgraph.getEdges();
        // Assign event time (=now) for every edge and print this list
        List<EdgeEvent> edgeEvents = new ArrayList<>();
        for (int i = 0; i < graphSize; i++) {
            edgeEvents.add(new EdgeEvent(edgeList.get(i)));
            System.out.println("Edge: " + edgeEvents.get(i).getEdge().getOriginVertex() + " "
                    + edgeEvents.get(i).getEdge().getDestinVertex() + " -- Time: " + edgeEvents.get(i).getEventTime());
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
                .timeWindow(Time.milliseconds(10));

        // Process all Windows into a "new" DataStream
        DataStream<Tuple2<List<EdgeEvent>, Integer>> processedWindowedEdges = windowedEdgeStream
                //.trigger(CountTrigger.of(10))
                .process(new ProcessEdgeWindow() {
                });

        processedWindowedEdges.print();

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

        // Match Function to connect broadcast (state) and edges
        MatchFunctionEdgeEvents matchRules = new MatchFunctionEdgeEvents();
        matchRules.setRound(1);

        DataStream<Tuple2<Integer, List<Integer>>> outputRules = edgeStreamForPartitioning
                .connect(broadcastRulesStream)
                .process(matchRules);

        //outputRules.print();



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
        test123.print();*/



        // ### Finally, execute the job in Flink
        env.execute();

    } // close main method






}








//        KeyedStream<EdgeSimple, Integer> edgeKeyedStream = windowedEdges
//                //.rebalance()                               // needed to increase the parallelism
//                .flatMap(new FlatMapFunction<EdgeSimple, Object>() {
//                })
//                .keyBy(EdgeSimple::getOriginVertex);
//
//        edgeKeyedStream.print();

/*        // Match Function to connect broadcast (state) and edges
        MatchFunctionRule matchRules = new MatchFunctionRule();
        matchRules.setRound(1);

        DataStream<Tuple2<Integer, List<Integer>>> outputRules = edgeKeyedStream
                .connect(broadcastRulesStream)
                .process(matchRules);*/

//outputRules.print();



/*        outputRules
                .connect(broadcastRulesStream)
                .process(matchRules);

        outputRules.print();*/
/*
        // ##### ROUND 3 #####
        MatchFunctionRule matchRules3 = new MatchFunctionRule();
        matchRules3.setRound(3);

        BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream3 = outputRules2
                .flatMap(new FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>>() {
                    @Override
                    public void flatMap(Tuple2<Integer, List<Integer>> value, Collector<Tuple2<Integer, List<Integer>>> out) {
                        out.collect(value);
                    }
                })
                .setParallelism(2)
                .broadcast(rulesStateDescriptor);

        KeyedStream<EdgeSimple, Integer> edgeKeyedStream3 = env.fromCollection(edgeList.subList(10,15))
                .rebalance()                               // needed to increase the parallelism
                .map(edgeSimple -> edgeSimple)
                .setParallelism(2)
                .keyBy(EdgeSimple::getOriginVertex);

        DataStream<Tuple2<Integer, List<Integer>>> outputRules3 = edgeKeyedStream3
                .connect(broadcastRulesStream3)
                .process(matchRules3);

        //outputRules3.print();
*/


//System.out.println(env.getExecutionPlan());






















 /*
        List<Tuple2<Integer, List<Integer>>> stateList2 = new ArrayList<>();
        stateList2.add(new Tuple2<>(new Integer(-1), stateArray));

        BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream2 = env.fromCollection(stateList2)
                .flatMap(new FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>>() {
                    @Override
                    public void flatMap(Tuple2<Integer, List<Integer>> value, Collector<Tuple2<Integer, List<Integer>>> out) {
                        out.collect(value);
                    }
                 })
                .setParallelism(2)
                .broadcast(rulesStateDescriptor);

        // Stream of with window-sized amount of edges
        KeyedStream<EdgeSimple, Integer> edgeKeyedStream2 = env.fromCollection(keyedInput.subList(5,9))
                .rebalance()                               // needed to increase the parallelism
                .map(edgeSimple -> edgeSimple)
                .setParallelism(2)
                .keyBy(EdgeSimple::getOriginVertex);

        DataStream<String> output2 = edgeKeyedStream2
                .connect(broadcastRulesStream2)
                .process(matchFunction);

        //DataStream<String> sideOutputStream = output.getSideOutput(outputTag);
        //output.print();
        outputRules.print();*/



//outputRules2.print();

/*                .process(new ProcessWindowFunction() {
                    @Override
                    public void process(Object o, Context context, Iterable elements, Collector out) throws Exception {
                        KeyedStream<EdgeSimple, Integer> edgeKeyedStream = env.fromCollection(edgeList.subList(5, 10))
                                .rebalance()                               // needed to increase the parallelism
                                .map(edgeSimple -> edgeSimple)
                                .setParallelism(2)
                                .keyBy(EdgeSimple::getOriginVertex);

                        out.collect(new DataStream<Tuple2<EdgeSimple>>());
                    }
                })*/

/*
                        BroadcastStream < Tuple2 < Integer, List < Integer >>> broadcastRulesStream = env.fromCollection(stateList)
                                //BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream2 = outputRules
                                .flatMap(new FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>>() {
                                    @Override
                                    public void flatMap(Tuple2<Integer, List<Integer>> value, Collector<Tuple2<Integer, List<Integer>>> out) {
                                        out.collect(value);
                                    }
                                })
                                .setParallelism(2)
                                .broadcast(rulesStateDescriptor);

        KeyedStream<EdgeSimple, Integer> edgeKeyedStream = env.fromCollection(edgeList.subList(5, 10))
                .rebalance()                               // needed to increase the parallelism
                .map(edgeSimple -> edgeSimple)
                .setParallelism(2)
                .keyBy(EdgeSimple::getOriginVertex);
*/

        /*
        CoProcessFunction<EdgeEvent, Tuple2<Integer, List<Integer>>, Tuple2<EdgeSimple, List<Integer>>> testCoProcess = new CoProcessFunction<EdgeEvent, Tuple2<Integer, List<Integer>>, Tuple2<EdgeSimple, Integer>>() {
            @Override
            public void processElement1(EdgeEvent value, Context ctx, Collector<Tuple2<EdgeSimple, Integer>> out) throws Exception {
                out.collect(new Tuple2<>(value.getEdge(), 100));
            }

            @Override
            public void processElement2(Tuple2<Integer, List<Integer>> value, Context ctx, Collector<Tuple2<EdgeSimple, Integer>> out) throws Exception {
                out.collect(new Tuple2<>(new EdgeSimple(200, 400), 800));
            }
        };
*/
/*        DataStream<Tuple2<Integer, List<Integer>>> outputRules = edgeEventStream
                .connect(broadcastRulesStream)
                .process(matchRules);*/

//outputRules2.print();

/*

        // Initialize "Match Function" for Broadcast Stream
        MatchFunctionRule matchRules = new MatchFunctionRule();
        matchRules.setRound(1);

        // create 1 sample "state" for Vertex 1, appearing in partition 1
        List<Integer> stateArray = new ArrayList<>(); stateArray.add(-1); stateArray.add(-1);
        List<Tuple2<Integer, List<Integer>>> stateList = new ArrayList<>();
        stateList.add(new Tuple2<>(new Integer(-1), stateArray));



        // Stream with Tuple2<EdgeEvent,"originVertex">
        // Sum up origin Vertex per Window (in total, it MUST add up to "graphSize" variable
        DataStream<Tuple2<EdgeEvent, Integer>> windowTestStream2 = edgeEventStream
                .map(new MapFunction<EdgeEvent, Tuple2<EdgeEvent, Integer>>() {
                    @Override
                    public Tuple2<EdgeEvent, Integer> map(EdgeEvent value)
                            throws Exception {
                        return new Tuple2<>(value,value.getEdge().getOriginVertex());
                    }
                });

*//*        KeyedStream<EdgeEvent, Integer> testWindow = edgeEventStream
                .map()
                .keyBy(1) // keyBy first Vertex ("originVertex");
                .timeWindow(Time.milliseconds(5))
                .sum(1);*//*

        //windowTestStream3.print();

        // ##### ROUND 1 #####
        // Stream of state table, based on an ArrayList
        BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream = env.fromCollection(stateList)
                .flatMap(new FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>>() {
                    @Override
                    public void flatMap(Tuple2<Integer, List<Integer>> value, Collector<Tuple2<Integer, List<Integer>>> out) {
                        out.collect(value);
                    }
                })
                .setParallelism(2)
                .broadcast(rulesStateDescriptor);

        KeyedStream<EdgeSimple, Integer> edgesWindowed = edgeEventStream
                //.rebalance()                               // needed to increase the parallelism
                .map(edgeSimple -> edgeSimple)
                .setParallelism(2)
                .keyBy(EdgeSimple::getOriginVertex);



        // Stream of with window-sized amount of edges
        KeyedStream<EdgeSimple, Integer> edgeKeyedStream = env.fromCollection(edgeList.subList(0,5))
                //.rebalance()                               // needed to increase the parallelism
                .map(edgeSimple -> edgeSimple)
                .setParallelism(2)
                .keyBy(EdgeSimple::getOriginVertex);

*//*        DataStream<EdgeSimple> vehicleCount = env.fromCollection(edgeStream)
                .keyBy(EdgeSimple::getOriginVertex)
                .countWindow(5)
                .sum(1);

        vehicleCount.print();*//*
        //edgeKeyedStream.timeWindowAll(Time.seconds(5));

        DataStream<Tuple2<Integer, List<Integer>>> outputRules = edgeKeyedStream
                //.timeWindow(Time.seconds(2))
                .connect(broadcastRulesStream)
                .process(matchRules);

        //outputRules.print();

*//*        KeySelector abc = new KeySelector<Tuple2<Integer, List<Integer>>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, List<Integer>> value) throws Exception {
                return value.f0;
            }
        };
        //Tuple2<Integer, List<Integer>> value


        DataStream<Tuple2<Integer, List<Integer>>> streamOutput = DataStreamUtils
                .reinterpretAsKeyedStream(outputRules, abc);
*//*

        // ##### ROUND 1 #####
        MatchFunctionRule matchRules2 = new MatchFunctionRule();
        matchRules2.setRound(2);


        BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream2 = outputRules
                //BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream2 = outputRules
                .flatMap(new FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>>() {
                    @Override
                    public void flatMap(Tuple2<Integer, List<Integer>> value, Collector<Tuple2<Integer, List<Integer>>> out) {
                        out.collect(value);
                    }
                })
                .setParallelism(2)
                .broadcast(rulesStateDescriptor);

        KeyedStream<EdgeSimple, Integer> edgeKeyedStream2 = env.fromCollection(edgeList.subList(5,10))
                .rebalance()                               // needed to increase the parallelism
                .map(edgeSimple -> edgeSimple)
                .setParallelism(2)
                .keyBy(EdgeSimple::getOriginVertex);

        DataStream<Tuple2<Integer, List<Integer>>> outputRules2 = edgeKeyedStream2
                .connect(broadcastRulesStream2)
                .process(matchRules2);

        //outputRules2.print();

*//*        DataStream<Tuple2<Integer, List<Integer>>> streamOutput2 = DataStreamUtils
                .reinterpretAsKeyedStream(outputRules2, abc);*/

