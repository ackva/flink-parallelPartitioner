package org.myorg.quickstart.deprecated.old;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.deprecated.EdgeEventDepr;

import java.util.HashMap;

public class MapFunctionFrequency implements MapFunction<EdgeEventDepr, Tuple2<EdgeEventDepr, Integer>> {

    private HashMap<Integer, Integer> stateTable;

    public MapFunctionFrequency(KeyedStream inputStream, StreamExecutionEnvironment env) {
        stateTable = new HashMap<>();
    }

    //new MapFunction<EdgeEventDepr, Tuple2<EdgeEventDepr, Integer>>() {
        @Override
        public Tuple2 map(EdgeEventDepr edge) throws Exception {
            Integer[] vertices = new Integer[2];
            vertices[0] = edge.getEdge().getOriginVertex();
            vertices[1] = edge.getEdge().getDestinVertex();
            int highest = 0;

            // Delete if "tag" is used
            int mostFreq = edge.getEdge().getOriginVertex();

            // Tagging for partitions

            int count;
            // Loop over both vertices and see which one has the higher degree (if equal, the left vertex "wins").
            for (int i = 0; i < 2; i++) {
                if (stateTable.containsKey(vertices[i])) {
                    count = stateTable.get(vertices[i]);
                    count++;
                    stateTable.put(vertices[i], count);
                } else {
                    stateTable.put(vertices[i], 1);
                    count = 1;
                }
                if (count > highest) {
                    highest = count;
                    mostFreq = vertices[i];
                    ;
                    //tag = (int) tuple.getField(i) % partitions;
                }
            }
            System.out.println(edge.getEdge().getOriginVertex() + " " + edge.getEdge().getDestinVertex() + " - " + mostFreq + " :::: " + edge);
            return new Tuple2<>(edge, mostFreq);

        }

}