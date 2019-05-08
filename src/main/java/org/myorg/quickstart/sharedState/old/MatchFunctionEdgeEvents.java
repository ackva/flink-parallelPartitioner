package org.myorg.quickstart.sharedState.old;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.sharedState.EdgeSimple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.myorg.quickstart.sharedState.old.PartitionWithBroadcast.tupleTypeInfo;

public class MatchFunctionEdgeEvents extends KeyedBroadcastProcessFunction<Integer, EdgeSimple, Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>> {

    private int counter = 0;
    private String processedEdges = "Edges processed by: ";
    private String processedBroadcastElements = "BroadcastRules processed by: ";
    private int round;

    public void setRound(int round) {
        this.round = round;
    }

    private final MapStateDescriptor<String, Tuple2<Integer, List<Integer>>> broadcastStateDescriptor =
            new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, tupleTypeInfo);

    @Override
    public void processBroadcastElement(Tuple2<Integer, List<Integer>> broadcastElement, Context ctx, Collector<Tuple2<Integer, List<Integer>>> out) throws Exception {

        System.out.println("R_"+ round + ":: Broadcasting Vertex " + broadcastElement.f0);

        if (broadcastElement.f0 != -1) {
            boolean inList = false;
            //ctx.getBroadcastState(broadcastStateDescriptor).put("Entry_" + counterBroadcast++, broadcastElement);
            for (Map.Entry<String, Tuple2<Integer, List<Integer>>> stateEntry : ctx.getBroadcastState(broadcastStateDescriptor).entries()) {
                if (stateEntry.getValue().f0 == broadcastElement.f0) {
                    inList = true;
                }

            }
            if (inList == false) {
                //ctx.getBroadcastState(broadcastStateDescriptor).put("Entry_" + counterBroadcast++, broadcastElement);
                ctx.getBroadcastState(broadcastStateDescriptor).put(broadcastElement.f0.toString(), broadcastElement);
                //System.out.println("R_"+ round +":: RULE: Vertex " + broadcastElement.f0 + " to state table");
                counter++;
                System.out.println("R_"+ round +":: State Table after ADDING Vertex " + broadcastElement.f0 + " --> " + ctx.getBroadcastState(broadcastStateDescriptor).entries());
            }
        }

    }

    @Override
    public void processElement(EdgeSimple currentEdge, ReadOnlyContext ctx, Collector<Tuple2<Integer, List<Integer>>> out) throws Exception {

        System.out.println("R_"+ round +":: Processing EDGE: " + currentEdge.getOriginVertex() + " " + currentEdge.getDestinVertex());
        processedEdges = processedEdges + currentEdge.getOriginVertex() + "," + currentEdge.getDestinVertex() + "--";
        //System.out.println("R_"+ round +":: " + processedEdges);


        List<Integer> currentPartitions = new ArrayList<>();
        boolean addToList;

        // Iterate through both vertices of the current edge and compare it with the existing entries from the state table
        for (Integer currentVertex: currentEdge.getVertices()) {
            addToList = true;
            stateLoop:
            for (Map.Entry<String, Tuple2<Integer, List<Integer>>> stateEntry: ctx.getBroadcastState(broadcastStateDescriptor).immutableEntries()) {
                //System.out.println(stateEntry.getValue().f0 + " < stateF0 --- currentVertex > " + currentVertex);
                if (stateEntry.getValue().f0 == currentVertex) {
                    System.out.println("R_"+ round +":: current vertex: " + currentVertex);
                    addToList = false;
                    break stateLoop;
                }
            }
            if (addToList == true) {
                List<Integer> partitions = new ArrayList<>();
                partitions.add(choosePartition(currentVertex));
                Tuple2<Integer, List<Integer>> tuple = new Tuple2(currentVertex, partitions);
                out.collect(tuple);
                //System.out.println("Added vertex " + currentVertex + " to partition: " + partitions.get(0));
            }
        }
        //broadcastRules(currentEdge);
    }

    public int choosePartition(int vertexId) {
        Random rand = new Random();
        return rand.nextInt(2);

    }

    public void broadcastRules(EdgeSimple currentEdge) throws Exception {

        System.out.println("broadcastFunction called for " + currentEdge.getOriginVertex() + " " + currentEdge.getDestinVertex());

        // Do something


   /*      //System.out.println("broadcastFunction called for " + currentEdge.getOriginVertex() + " " + currentEdge.getDestinVertex());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // MapState Descriptor (as from data artisans)
        MapStateDescriptor<String, Tuple2<Integer, ArrayList<Integer>>> rulesStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                tupleTypeInfo
        );

       // SHOULD BE TEMPORARY
        // #### Create 1 sample "state" for Vertex 1, appearing in partition 1
        List<Integer> stateArray = new ArrayList<>();
        stateArray.add(currentEdge.getOriginVertex());
        stateArray.add(currentEdge.getDestinVertex());
        List<Tuple2<Integer, List<Integer>>> stateList = new ArrayList<>();
        stateList.add(new Tuple2<>(new Integer(-1), stateArray));

        List<EdgeSimple> fakeList = new ArrayList<>();
        fakeList.add(currentEdge);

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

        System.out.println("TEST TEST");
        outputRules.printPhaseOne();*/

    }

}

