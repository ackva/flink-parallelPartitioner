package org.myorg.quickstart.deprecated.old;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.deprecated.EdgeEvent;
import org.myorg.quickstart.deprecated.EdgeSimple;
import org.myorg.quickstart.deprecated.GraphCreator;

import java.util.ArrayList;
import java.util.List;

public class SimpleFlinkJobCaller {

    public static void main(String[] args) throws Exception {

        // Environment setup
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        List<EdgeEvent> edgeEventList = getGraph(5000);
        List<Tuple2<Long, Long>> longList = new ArrayList<>();
        for (EdgeEvent e: edgeEventList) {
           longList.add(new Tuple2<>((long) e.getEdge().getOriginVertex(),(long) e.getEdge().getDestinVertex()));
        }
        DataStream<Tuple2<String, String>> test = env.fromCollection(longList)
                .keyBy(0)
                .flatMap(new TestValueState());

        test.print();

        // ### Finally, execute the job in Flink*/
        env.execute();

    } // close main method

    public static List<EdgeEvent> getGraph(int graphSize) {
        System.out.println("Number of edges: " + graphSize);
        GraphCreator tgraph = new GraphCreator();
        tgraph.generateGraphOneTwoToAny(graphSize);
        List<EdgeSimple> edgeList = tgraph.getEdges();
        // Assign event time (=now) for every edge and printPhaseOne this list
        List<EdgeEvent> edgeEvents = new ArrayList<>();
        for (int i = 0; i < graphSize; i++)
            edgeEvents.add(new EdgeEvent(edgeList.get(i)));

        return  edgeEvents;
    }

}
