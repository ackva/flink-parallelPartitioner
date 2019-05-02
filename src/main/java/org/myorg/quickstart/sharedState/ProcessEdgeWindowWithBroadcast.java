package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;


public class ProcessEdgeWindowWithBroadcast extends ProcessWindowFunction<EdgeEvent, Tuple2<List<EdgeEvent>, Integer>, Integer, TimeWindow> {

    //    private static ClassLoader

    final static Class<Tuple2<Integer, ArrayList<Integer>>> typedTuple = (Class<Tuple2<Integer, ArrayList<Integer>>>) (Class<?>) Tuple2.class;

    final static TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<>(
            typedTuple,
            new GenericTypeInfo<>(Integer.class),
            new GenericTypeInfo<>(Integer.class)
    );

    public List<Tuple2<Integer, List<Integer>>> stateList = new ArrayList<>();
    public List<Integer> stateArray = new ArrayList<>();
    public List<EdgeEvent> edgesInWindow = new ArrayList<>();
    public BroadcastStream<Tuple2<Integer,List<Integer>>> broadcastStream;
    public StreamExecutionEnvironment env;

    public ProcessEdgeWindowWithBroadcast() {
    }

    public ProcessEdgeWindowWithBroadcast(BroadcastStream<Tuple2<Integer,List<Integer>>> broadcastStream, StreamExecutionEnvironment env) {
        stateArray.add(-1);
        stateArray.add(-1);
        stateList.add(new Tuple2<>(new Integer(-1), stateArray));
        this.broadcastStream = broadcastStream;
        this.env = env;
    }


    public ProcessEdgeWindowWithBroadcast(StreamExecutionEnvironment env) {
        stateArray.add(-1);
        stateArray.add(-1);
        stateList.add(new Tuple2<>(new Integer(-1), stateArray));
        this.env = env;
    }
    //HashMap placedEdges = new HashMap();
    //HashMap vertexTable = new HashMap();



    public void process(Integer key, Context context, Iterable<EdgeEvent> edgeIterable, Collector<Tuple2<List<EdgeEvent>, Integer>> out) throws Exception {
//      int counter = 0;

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