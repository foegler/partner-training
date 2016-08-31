/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package PartnerTraining;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KeepTruckin Exercise 2
 * 
 * This exercise introduces using the TextIO class to read and write to text files.
 * 
 * This exercise also introduces the concept of "side outputs".  Side outputs
 * allow you to output multiple pCollections from the application of a single ParDo.
 * In this example, a ParDo with a primary output of the parsed log data also produces
 * a side output of the unparsable log lines for error reporting.
 * 
 */
public class Exercise2 {
	final static TupleTag<PackageActivityInfo> packageObjects = new TupleTag<PackageActivityInfo>() {
	};
	final static TupleTag<String> extraLines = new TupleTag<String>() {
	};

	private static final Logger LOG = LoggerFactory.getLogger(Exercise2.class);

	static class ParseLine extends DoFn<String, PackageActivityInfo> {
		private final Aggregator<Long, Long> invalidLines = createAggregator("invalidLogLines", new Sum.SumLongFn());

		@Override
		public void processElement(ProcessContext c) {
			String logLine = c.element();
			LOG.info("Parsing log line: " + logLine);
			PackageActivityInfo info = PackageActivityInfo.Parse(logLine);
			if (info == null) {
				invalidLines.addValue(1L);
				c.sideOutput(extraLines, logLine);
			} else {
				c.output(info);
			}
		}
	}

	public static void main(String[] args) {
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
		PCollectionTuple results = p.apply(TextIO.Read.from("/Users/foegler/Documents/package_log.txt"))
				.apply(ParDo.withOutputTags(packageObjects, TupleTagList.of(extraLines)).of(new ParseLine()));

		results.get(packageObjects).apply(ParDo.of(new DoFn<PackageActivityInfo, String>() {
			@Override
			public void processElement(ProcessContext c) {
				c.output(c.element().toString());
			}
		})).apply(TextIO.Write.named("WriteMyFile").to("/Users/foegler/Documents/package_out.txt"));

		results.get(extraLines)
				.apply(TextIO.Write.named("WriteMyFile").to("/Users/foegler/Documents/package_bad_lines.txt"));
		p.run();
	}
}
