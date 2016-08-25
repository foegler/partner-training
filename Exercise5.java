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

package dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A starter example for writing Google Cloud Dataflow programs.
 *
 * <p>
 * The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>
 * To run this starter example locally using DirectPipelineRunner, just execute
 * it without any additional parameters from your favorite development
 * environment.
 *
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform,
 * you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 * --runner=BlockingDataflowPipelineRunner
 */
public class Exercise5 {
	final static TupleTag<PackageActivityInfo> packageObjects = new TupleTag<PackageActivityInfo>() {
	};
	final static TupleTag<String> extraLines = new TupleTag<String>() {
	};

	private static final Logger LOG = LoggerFactory.getLogger(Exercise3.class);

	static class ParseLine extends DoFn<String, PackageActivityInfo> {
		private final Aggregator<Long, Long> invalidLines = createAggregator("invalidLogLines",
				new Sum.SumLongFn());

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
		Pipeline p = Pipeline
				.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
		p.getOptions().setTempLocation("gs://clouddfe-laraschmidt/temp/");
		PCollectionTuple results = p
				.apply(TextIO.Read.from("gs://clouddfe-laraschmidt/package_log.txt"))
				.apply(ParDo.withOutputTags(packageObjects, TupleTagList.of(extraLines))
						.of(new ParseLine()));

		// Build the table schema for the output table.
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("packageId").setType("STRING"));
		fields.add(new TableFieldSchema().setName("location").setType("STRING"));
		TableSchema schema = new TableSchema().setFields(fields);

		results.get(extraLines).apply(TextIO.Write.named("WriteMyFile")
				.to("gs://clouddfe-laraschmidt/package_bad_lines.txt"));
		
		results.get(packageObjects).apply(ParDo.of(new DoFn<PackageActivityInfo, TableRow>() {
			@Override
			public void processElement(ProcessContext c) {
				c.output(new TableRow().set("packageId", c.element().packageId).set("location",
						c.element().location));
			}
		})).apply(BigQueryIO.Write.named("BigQuery-Write")
				.to("google.com:clouddfe:partner_training_dataset.tablez")
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.withSchema(schema));

		p.run();
	}
}
