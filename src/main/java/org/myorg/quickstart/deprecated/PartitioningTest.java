package org.myorg.quickstart.deprecated;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.jobstatistics.JobStatistics;
import org.myorg.quickstart.jobstatistics.JobSubtask;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

import static java.lang.System.out;

public class PartitioningTest {


    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Make parameters available in Web Interface
        env.getConfig().setGlobalJobParameters(params);

        // Set level of parallelism (hardcoded at the moment)
        //env.getConfig().setParallelism(2);

        // Get input data
        //DataStream<String> streamInput = env.readTextFile(params.get("input"));
        DataStream<String> streamInput = env.readTextFile(params.get("input"));

        // Get timestamp for logging
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());

        // Initialize state table (Columns: Vertex, Occurrence)
        HashMap<String, Long> stateTable = new HashMap<>();

        // FlatMap function to create proper tuples (Tuple2) with both vertices, as in input file --> 22,45 --> Tuple2(22,45)
        SingleOutputStreamOperator edges = streamInput.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String streamInput, Collector<Tuple2<String, String>> out) {

                // normalize and split the line
                String[] tokens = streamInput.replaceAll("\\s+", "").split("\\r?\\n");
                // emit the pairs
                for (String token : tokens) {
                    if (token.length() > 0) {
                        String[] elements = token.split(",");
                        String t0 = elements[0];
                        String t1 = elements[1];
                        out.collect(new Tuple2<>(t0, t1));
                        Arrays.fill(elements, null);
                    }
                }
            }
        });

        // Tag edges (results from FlatMap) by constructing a state table (hash map) that keeps track of vertices and their occurrences
        SingleOutputStreamOperator taggedEdges = edges.map(new MapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3 map(Tuple2<String, String> tuple) throws Exception {
                String[] vertices = new String[2];
                vertices[0] = tuple.f0;
                vertices[1] = tuple.f1;
                return new Tuple3<>(vertices[0], vertices[1], Integer.parseInt(vertices[0]));

            }
        });

        DataStream partitionedEdges = taggedEdges.partitionCustom(new SmartPartitionerOld.PartitionByTag(), 2);
        //DataStream keyedEdges = partitionedEdges.keyBy(2);

        //edges.printPhaseOne();
        partitionedEdges.print();

        // Execute program
        JobExecutionResult jobResult = env.execute("Streaming Items and Partitioning");


        // Get Job Metrics; depends on local or cluster execution
        if (params.get("isLocalTest").equals("yes")) {
            out.println("No metrics because run locally");
        } else if (params.get("isLocalTest").equals("no")) {
            // Get Job Metrics
            String jobId = jobResult.getJobID().toString();
            JobStatistics job = new JobStatistics(jobId);
            job.populateJobAttributes();
            String csvLogging = getJobStatistics(jobId);
            try {
                Files.write(Paths.get(params.get("output")), (csvLogging + System.lineSeparator()).getBytes(),
                        StandardOpenOption.APPEND); //StandardOpenOption.CREATE,
            } catch (Exception e) {
                out.println("Job Time append operation failed " + e.getMessage());
            }
        } else {
            out.println("Please specify argument 'isLocalTest'");
        }


    }

    public static String getJobStatistics(String jobId) throws Exception {
        JobStatistics jobStats = new JobStatistics(jobId);
        jobStats.populateJobAttributes();

        String csvLogging = "";
        String subtasksPrint = "";
        for (JobSubtask sub : jobStats.getVertices().get(1).getSubtasks()) {
            subtasksPrint = subtasksPrint + sub.getWriteRecords() + ",";
        }

        System.out.println("Job Statistics");
        System.out.println("JobId, JobName, Input, Parallelism, Duration, Subtask_ignore, Subtask_0, Subtask_1, Subtask_2, Subtask_3, Subtask_4");
        csvLogging = jobStats.getJobId() + "," + jobStats.getName() + "," + jobStats.getInputFile() + ","
                + jobStats.getParallelism() + "," + jobStats.getDuration() + "," + subtasksPrint;
        System.out.println(csvLogging);

        return csvLogging;
        // Some runtime statistics
    }

    }

