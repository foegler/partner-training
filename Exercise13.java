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

import java.util.ArrayList;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;

import PartnerTraining.Exercise11Part3.WindowCountsToRows;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keep Truckin Exercise 11 Part 3
 * 
 * Use Windows to group by hour and write to BigQuery.
 * This exercise is meant to prepare for the transition
 * to Streaming Dataflows, where writing to BigQuery and
 * PubSub are far more comman than text files.
 */
@SuppressWarnings("serial")
public class Exercise13 {
	private static final Logger LOG = LoggerFactory.getLogger(Exercise11Part3.class);

	static class WindowCountsToRows extends DoFn<KV<String, Long>, TableRow> implements DoFn.RequiresWindowAccess {
		@Override
		public void processElement(ProcessContext c) {
			c.output(
				new TableRow()
					.set("location", c.element().getKey())
					.set("count", c.element().getValue())
					.set("timestamp", ((IntervalWindow) c.window()).start().getMillis()));
			}
	 	}
	
	// TODO(laraschmidt): refactor this out or delete this.
	public static class UnboundedGenerator
			extends PTransform<PBegin, PCollection<PackageActivityInfo>> {
		@Override
		public PCollection<PackageActivityInfo> apply(PBegin input) {
			return input.apply(Read.from(new GenericUnboundedSource()));
		}
	}
	
	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		// Convert to DataflowPipelineOptions and set streaming to true
		DataflowPipelineOptions dataflowOptions= options.as(DataflowPipelineOptions.class);
		dataflowOptions.setStreaming(true);
		
		// Convert to ExerciseOptions and set the dataset that this pipeline uses.
	    ExerciseOptions eo = dataflowOptions.as(ExerciseOptions.class);
	    eo.setDataset("laradataset");
		Pipeline p = Pipeline.create(eo);
		
		// Define the table schema for the BigQuery output table.
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("location").setType("STRING"));
		fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
		TableSchema schema = new TableSchema().setFields(fields);
		
		// Read the log lines from file.
		PCollection<KV<String, Long>> thing = p.apply(new UnboundedGenerator())
		// Parse the log lines into objects.
		// .apply(ParDo.of(new PackageActivityInfo.ParseLine()))
		 // Since bounded data sources do not contain timestamps, we need to
		 // emit each element from the PCollection with the time as the
		 // timestamp.
		 .apply(ParDo.of(new DoFn<PackageActivityInfo, PackageActivityInfo>() {
		    public void processElement(ProcessContext c) {
	            // Extract the timestamp from log entry we're currently processing.
	            Instant logTimeStamp = new Instant(((PackageActivityInfo) c.element()).getTime());
	            // Use outputWithTimestamp to emit the log entry with timestamp attached.
	            c.outputWithTimestamp(c.element(), logTimeStamp);
		     }}))
		// Define a hour long window for the data.
		 .apply(Window.<PackageActivityInfo>into(
				 FixedWindows.of(Duration.standardMinutes(1))))
		// Extract the location key from each object.
		 .apply(WithKeys
				.of(new SerializableFunction<PackageActivityInfo, String>() {
					public String apply(PackageActivityInfo s) {
						return s.getLocation();
					}
				}))			 
		// Count the objects from the same hour, per location.
		 .apply(Count.<String, PackageActivityInfo> perKey());
		// Format the output.  Need to use a ParDo since need access
		// to the window time.
		thing.apply(ParDo.of(new WindowCountsToRows()))
		  // Write the Table rows to the output table.  The dataset must already exist
		  // before executing this command.  If you have not created it, use the BigQuery
		  // UI in the Developers Console to create the dataset.
		  //
		  // With the option CREATE_IF_NEEDED, the table will be created if it doesn't
		  // already exist.
		  // Use the BigQuery Query UI to verify your export:
		  // SELECT * FROM partner_training_dataset.package_info LIMIT 5;
		  .apply(BigQueryIO.Write.named("BigQuery-Write")
				.to("laradataset.package_counts2")
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withSchema(schema));
		p.run();
	}
}