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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.*;


/*

Work is under construction

Use the "Program arguments":
--input input/streamInput.txt --output output/result

History:
-- originally created as SmartPartitioner
0.1 | 05/02/2019 - simple hash table and tagging based on "more frequent" vertex in hash table
0.2 | 06/02/2019 - custom Partitioner which partitions based on tagged value
0.3 | 11/02/2019 - under construction
-- 14/02/2019 --   FORKED into new "PrePartitioner"

 */

public class PrePartitioner {

	DataStream<String> streamInput;
	private int partitions;
	private StreamExecutionEnvironment env;

	public PrePartitioner(DataStream streamInput, int partitions, StreamExecutionEnvironment env) {
		this.streamInput = streamInput;
		this.partitions = partitions;
		this.env = env;
	}



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





	public ArrayList<Tuple2<String, String>> getVertices() {

		ArrayList<Tuple2<String, String>> edges = new ArrayList<>();
		Tuple2<String, String> tuple = new Tuple2();
		String input = streamInput.toString();
		String[] tokens = input.replaceAll("\\s+", "").split("\\r?\\n");
		// emit the pairs
		for (String token : tokens) {
			if (token.length() > 0) {
				String[] elements = token.split(",", 2);
				tuple.f0 = elements[0];
				tuple.f1 = elements[1];
				edges.add(tuple);
			}
		}

		return edges;
	}
}