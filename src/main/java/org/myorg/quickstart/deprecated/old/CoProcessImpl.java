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
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.deprecated.EdgeEventDepr;
import org.myorg.quickstart.deprecated.EdgeSimple;
import org.myorg.quickstart.deprecated.GraphCreator;

import java.util.*;

/**
 *
 * Idea:
 * Partition a graph by using a CoProcessFunction which connects an EdgeDepr stream with a "state" Stream.
 * This does not really work.
 *
 * We found out that the NEWLY created Streams above, inside "processElement()" are not created/executed (Not sure which Java terminology to use).
 * They are not fetched by the initial job DAG
 * Consequently, it would be required to create a new StreamingEnvironment. This is not recommended, as written in the Google Doc.

 *
 */


public class CoProcessImpl {

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    final static Class<Tuple2<Integer, ArrayList<Integer>>> typedTuple = (Class<Tuple2<Integer, ArrayList<Integer>>>) (Class<?>) Tuple2.class;

    final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(
            typedTuple,
            new GenericTypeInfo<>(Integer.class),
            new GenericTypeInfo<>(Integer.class)
    );

    public static void main(String[] args) throws Exception {

        // Argument fetching
        int graphSize = 20;

        // Environment setup
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // ### Generate graph and make "fake events" (for window processing)
        List<EdgeEventDepr> edges1 = getGraph(graphSize); // see below

        List<Tuple2<Integer, List<Integer>>> stateList = new ArrayList<>();
        for(int i =  0; i < 10; i++) {
            List<Integer> valueList = new ArrayList<>();
            for (int j = 0; j < 3; j++) {
                valueList.add(i+j);
            }
            stateList.add(new Tuple2<>(i,valueList));
        }

        //System.out.println(Arrays.asList(stateList));

        DataStream<Tuple2<Integer, List<Integer>>> ruleStream = env.fromCollection(stateList);

        List<Integer> keyedInput = new ArrayList<>();
        keyedInput.add(1); keyedInput.add(2);


        // ### Create EdgeDepr Stream from input graph
        // Assign timestamps to the stream
        KeyedStream<EdgeEventDepr,Integer> keyedEdgeStream = env.fromCollection(edges1)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<EdgeEventDepr>() {
                    @Override
                    public long extractAscendingTimestamp(EdgeEventDepr element) {
                        return element.getEventTime();
                    }
                })
                .keyBy(new KeySelector<EdgeEventDepr, Integer>() {
                    @Override
                    public Integer getKey(EdgeEventDepr value) throws Exception {
                        return value.getEdge().getOriginVertex();
                    }
                });


        DataStream<String> coProcessedStream = keyedEdgeStream
           .connect(ruleStream)
                .process(new CoProcessFunction<EdgeEventDepr, Tuple2<Integer, List<Integer>>, String>() {
                             int counter1 = 0;
                             int counter2 = 0;
                             int totalCounter = 0;
                             List<Tuple2<Integer, List<Integer>>> stateListSink = new ArrayList<>();

                             @Override
                             // EdgeDepr Stream
                             public void processElement1(EdgeEventDepr value, Context ctx, Collector<String> out) throws Exception {
                                 counter1++;
                                 out.collect(value.getEdge() + " _1_ " + counter1);
                             }

                             @Override
                             // Broadcast Elements
                             public void processElement2(Tuple2<Integer, List<Integer>> value, Context ctx, Collector<String> out) throws Exception {

                                 // Add state element to state list and printPhaseOne it
                                 stateListSink.add(value);
                                 counter2++;
                                 String state = "";
                                 for (Integer i : value.f1) {
                                     state = state + " " + i;
                                 }
                                 out.collect(value.f0 + " in: " + state + " -- counterBroadcast = " + counter2);

                                 // Whenever the fake "window" is full, a broadcast is done --- This does not work. See line 140ff
                                 if (stateListSink.size() % 2 == 0) {
                                     List<String> fakeEventStream = new ArrayList<>();
                                     fakeEventStream.add("fake");

                                     // MapState Descriptor (as from data artisans)
                                     MapStateDescriptor<String, Tuple2<Integer, List<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, tupleTypeInfo);

                                     BroadcastStream<Tuple2<Integer, List<Integer>>> ruleStreamBroad = env.fromCollection(stateListSink)
                                             //BroadcastStream<Tuple2<Integer, List<Integer>>> broadcastRulesStream2 = outputRules
                                             .flatMap(new FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>>() {
                                                 @Override
                                                 public void flatMap(Tuple2<Integer, List<Integer>> value, Collector<Tuple2<Integer, List<Integer>>> out) {
                                                     System.out.println("hello");
                                                     out.collect(value);
                                                 }
                                             })
                                             .setParallelism(2)
                                             .broadcast(rulesStateDescriptor);

                                     DataStream<String> fakeStream = env.fromCollection(fakeEventStream);
                                     fakeStream.print(); // this does not work -- see below

                                     // Match Function to connect broadcast (state) and edges
                                     MatchFunctionMockup matchRules = new MatchFunctionMockup();
                                     matchRules.setRound(1);

                                     // ************
                                     // We found out that the NEWLY created Streams above, inside "processElement()" are not created/executed (Not sure which Java terminology to use). They are not fetched by the initial job DAG
                                     // Consequently, it would be required to create a new StreamingEnvironment. This is not recommended, as written in the Google Doc.

/*
                        DataStream<Tuple2<Integer, List<Integer>>> testBroadcastStream = edgeKeyedStream3
                                .connect(ruleStreamBroad)
                                .process(matchRules); // This passed function needs to be changed theoretically, if we want to continue here.
*/

                                 }

                             }

                         });
        coProcessedStream.print();

        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    public static List<EdgeEventDepr> getGraph(int graphSize) {
        System.out.println("Number of edges: " + graphSize);
        GraphCreator tgraph = new GraphCreator();
        tgraph.generateGraphOneToAny(graphSize);
        List<EdgeSimple> edgeList = tgraph.getEdges();
        // Assign event time (=now) for every edge and printPhaseOne this list
        List<EdgeEventDepr> edgeEventDeprs = new ArrayList<>();
        for (int i = 0; i < graphSize; i++)
            edgeEventDeprs.add(new EdgeEventDepr(edgeList.get(i)));

        return edgeEventDeprs;
    }
}

