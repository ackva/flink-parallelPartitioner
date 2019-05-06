/*
package org.myorg.quickstart.sharedState;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

public class PartitionByFrequency {
*/
/*
Work is under construction
Use the "Program arguments":
--input input/streamInput.txt --output output/result
History:
0.1 | 05/02/2019 - simple hash table and tagging based on "more frequent" vertex in hash table
0.2 | 06/02/2019 - custom Partitioner which partitions based on tagged value
 *//*



    public static void main(String[] args) throws Exception {

        // Initialize state table (Columns: Vertex, Occurrence)
        HashMap<String, Long> stateTable = new HashMap<>();

        // Tag edges (results from FlatMap) by constructing a state table (hash map) that keeps track of vertices and their occurrences
        SingleOutputStreamOperator taggedEdges = edges.map(new MapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>>() {
            @Override public Tuple3 map(Tuple2<String, String> tuple) throws Exception {
                String[] vertices = new String[2];
                vertices[0] = tuple.f0;
                vertices[1] = tuple.f1;
                Long highest = 0L;

                // Delete if "tag" is used
                int mostFreq = Integer.parseInt(tuple.f0);

                // Tagging for partitions

                Long count;
                // Loop over both vertices and see which one has the higher degree (if equal, the left vertex "wins").
                for (int i = 0; i < 2; i++) {
                    if (stateTable.containsKey(vertices[i])) {
                        count = stateTable.get(vertices[i]);
                        count++;
                        stateTable.put(vertices[i], count);
                    } else {
                        stateTable.put(vertices[i], 1L);
                        count = 1L;
                    }
                    if (count > highest) {
                        highest = count;
                        mostFreq = Integer.parseInt(tuple.getField(i));;
                        //tag = (int) tuple.getField(i) % partitions;
                    }
                }

                //out.println(stateTable);
                Files.write(Paths.get("logFile.txt"), (stateTable + ";"+ System.lineSeparator()).getBytes(),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);

                return new Tuple3<>(tuple.f0,tuple.f1,mostFreq);

            }});

        // Partition edges
        int partitions = env.getConfig().getParallelism();

        DataStream partitionedEdges = taggedEdges.partitionCustom(new org.myorg.quickstart.SmartPartitioner.PartitionByTag(),2);

        // Emit results
        edges.print();
        //taggedEdges.print();
        partitionedEdges.print();

        // write results to log file on local disk
        Files.write(Paths.get("logFile.txt"), ("job_" + timeStamp + System.lineSeparator()).getBytes(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        // Execute program
        JobExecutionResult result = env.execute("Streaming Items and Partitioning");
        long executionTime = result.getNetRuntime();

        // Some runtime statistics
        try {
            String execTimeText = "job_" + timeStamp + "---"
                    + executionTime + "ms---"
                    + env.getConfig().getParallelism() + "_parallelism "
                    + "---";
            Files.write(Paths.get("jobExecTimes.txt"), (execTimeText + System.lineSeparator()).getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("Job Time append operation failed");
        }

    }

    public static class PartitionByTag implements Partitioner<Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            return key % numPartitions;
        }
    }


}*/
