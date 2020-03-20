/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.wordcount;

import com.ververica.flinktraining.util.LogSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * Implements the "WordCount" program that computes a simple word occurrence
 * histogram over two different inputs in a streaming fashion.
 */
public class WordCount {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// this input generating one line per second.
		DataStream<String> words1 = env.addSource(new ContinuousGeneratingLinesSource());
		// this input generating one word per second.
		DataStream<String> words2 = env.addSource(new ContinuousGeneratingWordsSource());

		// TODO: implement your own word count of the above inputs
		words1.connect(words2).flatMap(new Tokenizer()).keyBy(0).sum(1).addSink(new LogSink());

		// execute program
		env.execute("Two Streaming WordCount");
	}

	// *************************************************************************
	// UTILITY FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined CoFlatMapFunction. The function takes a line (String) from first input and
	 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}), and takes a word form second input and make the same pair as first input.
	 */
	public static final class Tokenizer implements CoFlatMapFunction<String, String, Tuple2<String, Integer>> {

		@Override
		public void flatMap1(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}

		@Override
		public void flatMap2(String value, Collector<Tuple2<String, Integer>> out) {
			out.collect(new Tuple2<>(value, 1));
		}
	}

	public static final class ContinuousGeneratingLinesSource implements SourceFunction<String> {
		private volatile boolean isRunning = true;

		private String[] data = new String[] {
				"Apache Flink is an open source platform for distributed stream and batch data processing.",
				"Flink’s core is a streaming dataflow engine that provides data distribution, communication," +
				" and fault tolerance for distributed computations over data streams.",
				"Flink builds batch processing on top of the streaming engine," +
				"overlaying native iteration support, managed memory, and program optimization."
		};

		@Override
		public void run(SourceFunction.SourceContext<String> context) {
			int i = 0;

			while (isRunning) {
				context.collect(data[i++ % data.length]);
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	public static final class ContinuousGeneratingWordsSource implements SourceFunction<String> {
		private volatile boolean isRunning = true;

		private String[] data = new String[] {
				"Flink",
				"takes",
				"over",
				"everything"
		};

		@Override
		public void run(SourceFunction.SourceContext<String> context) {
			int i = 0;
			while (isRunning) {
				context.collect(data[i++ % data.length]);
				try {
					Thread.sleep(1500);
				} catch (Exception e) {
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}
}
