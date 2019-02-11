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

package org.myorg.quickstart;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.utils.JaccardSimilarity;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.valueOf;
import static java.lang.System.out;
import static java.nio.file.Files.readAllLines;


/*

Work is under construction

Use the "Program arguments":
--input input/streamInput.txt --output output/result

History:
0.1 | 05/02/2019 - simple hash table and tagging based on "more frequent" vertex in hash table
0.2 | 06/02/2019 - custom Partitioner which partitions based on tagged value
0.3 | 11/02/2019 - under construction

 */

public class SmartPartitioner {


	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// Set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Make parameters available in Web Interface
		env.getConfig().setGlobalJobParameters(params);

		// Set level of parallelism (hardcoded at the moment)
		//env.getConfig().setParallelism(2);
		int parallelism = env.getConfig().getParallelism();

		// (debugging) Last Vertex
		/*
		7 edges			2,5
		107 edges 		27,45
		1300 edges		1466,1467
		129,000 edges	18770,18771
		 */
		int lastVertex1 = 1466;
		int lastVertex2 = 1467;
		int numberOfEdges = 107;


		// Get input data
		DataStream<String> streamInput = env.readTextFile(params.get("input"));

		// Get timestamp for logging
		String timeStamp = new SimpleDateFormat("yyyy.MM.dd-HH.mm.ss").format(new Date());
		String fileData = "";
		String filePath = "output/job_" + timeStamp + "_log.txt";
/*		try{
		Files.write(Paths.get(filePath), fileData.getBytes());
	} catch (IOException e) {
		out.println("Job Time append operation failed");
	}*/

		// Initialize state table (Columns: Vertex, Occurrence)
		LinkedHashMap<String, Long> stateTable = new LinkedHashMap<>();

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
		SingleOutputStreamOperator taggedEdges = edges.map(new MapFunction<Tuple2<String, String>, Tuple4<String, String, Integer, HashMap>>() {
			@Override public Tuple4 map(Tuple2<String, String> tuple) throws Exception {
				String[] vertices = new String[2];
				vertices[0] = tuple.f0;
				vertices[1] = tuple.f1;
				Long highest = 0L;
				// Delete if "tag" is used
				int mostFreq = Integer.parseInt(tuple.f0);

				// Tagging for partitions

				long count;
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

				Charset charset = StandardCharsets.UTF_8;
				String fileNameString = "jobHash.txt";
				File file = new File(fileNameString);
				//File file = new File("output/job_" + timeStamp + "_finalHash.txt");
				try{
					file.createNewFile();
				} catch (IOException e) {
					out.println("Maybe now?");
				}
				Path pathFinal = Paths.get(fileNameString);
				//Path pathFinal = Paths.get("output/job_" + timeStamp + "_finalHash.txt");
				Path pathLog = Paths.get(fileNameString);
				//Path pathLog = Paths.get("output/job_" + timeStamp + "_log.txt");
				List<String> oldStateTable = Files.readAllLines(pathLog,charset);
				double sizeOldStateTable = oldStateTable.size();
				double threshold = numberOfEdges/parallelism-0.2*numberOfEdges;
				out.println("current size: " + threshold);
				out.println("old size: " + sizeOldStateTable);

				//out.println(stateTable);
				/*if (parallelism == 1) {
					if (Integer.parseInt(vertices[0]) == lastVertex1 && Integer.parseInt(vertices[1]) == lastVertex2) {
						Files.write(Paths.get(filePath), (stateTable + ";" + System.lineSeparator()).getBytes(),
								StandardOpenOption.CREATE, StandardOpenOption.APPEND);
					}
				} else {
					Files.write(Paths.get(filePath), (stateTable + ";" + System.lineSeparator()).getBytes(),
							StandardOpenOption.CREATE, StandardOpenOption.APPEND);
				}*/
				if (sizeOldStateTable > threshold) {
					//JaccardSimilarity js = new JaccardSimilarity();
					double jaccardSimilarty = 0d;
					double jaccardSim = -1d;
					int highestJaccard = -1;
					for (int i = oldStateTable.size(); i > oldStateTable.size(); i--) {
						//jaccardSim = js.calculateJaccardSimilarity(stateTable.toString(), oldStateTable.get(i));
						out.println("Current HT: " + stateTable.toString());
						out.println("Old HT: " + oldStateTable.get(i));
						//out.println("Jaccard: " + js.calculateJaccardSimilarity(stateTable.toString(), oldStateTable.get(i)));
						if (jaccardSim > highestJaccard) {
							highestJaccard = i;
						}
					}
				}
				return new Tuple4<>(tuple.f0,tuple.f1,mostFreq, stateTable);
				//return new Tuple4<>("","",99999, stateTable);

			}});


		// Partition edges
		int partitions = env.getConfig().getParallelism();

		DataStream partitionedEdges = taggedEdges.partitionCustom(new PartitionByTag(),2);

		// Emit results
		edges.print();
		taggedEdges.print(timeStamp);
		//stateTableStuff.print();
		List<String> list = new ArrayList<>();

		// Execute program
		JobExecutionResult result = env.execute("Streaming Items and Partitioning");
		long executionTime = result.getNetRuntime();

		// Some runtime statistics
/*
		try {
			String execTimeText = "job_" + timeStamp + "---"
					+ executionTime + "ms---"
					+ env.getConfig().getParallelism() + "_parallelism "
					+ "---";
			Files.write(Paths.get("output/01_jobExecTimes.txt"), (execTimeText + System.lineSeparator()).getBytes(),
					StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		} catch (IOException e) {
			out.println("Job Time append operation failed");
		}
*/

		// Compare the output files
		CompareOutput comparator = new CompareOutput(filePath,env.getParallelism(),timeStamp);
/*		if (parallelism > 1) {
			if (comparator.checkEquality()) {
				out.println("Nice!");
			} else {
				out.println("something went wrong");
			}
		}*/

	}

	public static class PartitionByTag implements Partitioner<Integer> {
		@Override
		public int partition(Integer key, int numPartitions) {
			return key % numPartitions;
		}
	}

}





/*		SingleOutputStreamOperator stateTableStuff = taggedEdges.map(new MapFunction<Tuple4<String, String, Integer, HashMap>, String>() {
			@Override public String map(Tuple4<String, String, Integer, HashMap> tuple) throws Exception {
				Charset charset = StandardCharsets.UTF_8;
				Path path = Paths.get("output/job_" + timeStamp + "_finalHash.txt");
				String currentHashTable = "";
				currentHashTable = tuple.f3.toString();
				long sizeOfTable = tuple.f3.size();

				String content = "";
				out.println(sizeOfTable);


				if (sizeOfTable == 2) {
					Files.write(path, content.getBytes(charset));
					out.println("new file: " + content);
*//*				} else if (sizeOfTable == 3 || sizeOfTable < 4) {
					content = currentHashTable;
					Files.write(path, content.getBytes(charset));*//*
				} else if (sizeOfTable > 2) {
					List<String> oldStateTable = Files.readAllLines(path,charset);
					double jaccardSim = -1d;
					int highestJaccard = -1;
					for (int i = 0; i < oldStateTable.size(); i++) {
						jaccardSim = js.calculateJaccardSimilarity(currentHashTable, oldStateTable.get(i));
						out.println("Current HT: " + currentHashTable);
						out.println("Old HT: " + oldStateTable.get(i));
						out.println("Jaccard: " + js.calculateJaccardSimilarity(currentHashTable, oldStateTable.get(i)));
						if (jaccardSim > highestJaccard) {
							highestJaccard = i;
					}

					Files.write(path, content.getBytes(charset));

*//*

					// Calculate Jaccard Similarity
					jaccardSimilarty = js.calculateJaccardSimilarity(currentHashTable, currentHashTable + "abc");
					String aa = Double.toString(jaccardSimilarty) + "__ similarity, " + currentHashTable;
*//*


					//Files.write(path, content.getBytes(charset));
					*//*Files.write(Paths.get(filePath), (stateTable + ";" + System.lineSeparator()).getBytes(),
							StandardOpenOption.CREATE, StandardOpenOption.APPEND);*//*
				}








*//*				Path fileName2 = new Path["output/job_" + timeStamp + "_finalHash.txt"];
				for (int i = 0; i < filenames.size(); i++) {
					paths[i] = Paths.get(filenames.get(i));
					files.add(i, readAllLines(paths[i], StandardCharsets.UTF_8));

				Path path = Paths.get(fileName);
				byte[] bytes = Files.readAllBytes(path);
				List<String> allLines = Files.readAllLines(path, StandardCharsets.UTF_8);*//*




 *//*				//out.println(stateTable);
				if (parallelism == 1) {
					if (Integer.parseInt(vertices[0]) == lastVertex1 && Integer.parseInt(vertices[1]) == lastVertex2) {
						Files.write(Paths.get(filePath), (stateTable + ";"+ System.lineSeparator()).getBytes(),
								StandardOpenOption.CREATE, StandardOpenOption.APPEND);
					}
				} else {
					Files.write(Paths.get(filePath), (stateTable + ";" + System.lineSeparator()).getBytes(),
							StandardOpenOption.CREATE, StandardOpenOption.APPEND);
				}*//*
				return new String("abcd");

			}});*/

