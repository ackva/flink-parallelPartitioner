/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart.deprecated;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.graph.GraphCsvReader;
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
import static java.lang.System.out;
import static java.lang.System.setOut;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/*
Work is under construction
Use the "Program arguments":
--input input/streamInput.txt --output output/result
History:
0.1 | 05/02/2019 - simple hash table and tagging based on "more frequent" vertex in hash table
0.2 | 06/02/2019 - custom Partitioner which partitions based on tagged value
 */

public class SmartPartitionerOld {


	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// Set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Make parameters available in Web Interface
		env.getConfig().setGlobalJobParameters(params);

		// Set level of parallelism (hardcoded at the moment)
		//env.getConfig().setParallelism(4);

		// Get input data
		//DataStream<String> streamInput = env.readTextFile(params.get("input"));
		DataStream<String> streamInput = env.readTextFile(params.get("input"));

		// Get timestamp for logging
		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());

		// Initialize state table (Columns: VertexDepr, Occurrence)
		HashMap<String, Long> stateTable = new HashMap<>();

		// FlatMap function to create proper tuples (Tuple2) with both vertices, as in input file
		SingleOutputStreamOperator edges = streamInput.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
			@Override public void flatMap(String streamInput, Collector<Tuple2<String, String>> out) {

				// normalize and split the line
				String[] tokens = streamInput.replaceAll("\\s+","").split("\\r?\\n");
				// emit the pairs
				for (String token:tokens) {
					if (token.length() > 0) {
						String[] elements = token.split(",",2);
						String t0 = elements[0];
						String t1 = elements[1];
						out.collect(new Tuple2<>(t0, t1));
						Arrays.fill(elements, null);

					}

				}
			}});

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

		DataStream partitionedEdges = taggedEdges.partitionCustom(new PartitionByTag(),2);

		// Emit results
		edges.print();
		//taggedEdges.printPhaseOne();
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
			out.println("Job Time append operation failed");
		}

	}

	public static class PartitionByTag implements Partitioner<Integer> {
		@Override
		public int partition(Integer key, int numPartitions) {
			return key % numPartitions;
		}
	}


}