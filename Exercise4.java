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
 * 
 * Keep Truckin Exercise 4
 * 
 * Integrate CLoud Dataflow with BigQuery.
 * 
 * In this exercise, you will export results from a Dataflow
 * job to a BigQuery Table.
 */
public class Exercise4 {
	private static final Logger LOG = LoggerFactory.getLogger(Exercise4.class);

	public static void main(String[] args) {
		Pipeline p = Pipeline
				.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

		String filePath = "gs://deft-foegler/";
		if (p.getOptions().getRunner().getSimpleName().equals("DirectPipelineRunner")){
			// The location of small test files on your local machine
			filePath = "/Users/foegler/Documents/";
		} else {
			// Your staging location or any other cloud storage location where you will upload files.
			filePath = "gs://deft-foegler/"; 
		}
		
		// BigQueryIO will not use the staging location for temporary
		// file storage, so need to define the option here.
		p.getOptions().setTempLocation("gs://deft-foegler/temp/");
		
		// Define the table schema for the BigQuery output table.
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("packageId").setType("STRING"));
		fields.add(new TableFieldSchema().setName("location").setType("STRING"));
		fields.add(new TableFieldSchema().setName("truckId").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
		TableSchema schema = new TableSchema().setFields(fields);
		
		// Use TextIO to read in a log file from disk.
		p.apply(TextIO.Read.from(filePath + "package_log.txt"))
		// Apply a ParDo using the parsing function provided in PackageActivityInfo.	
		 .apply(ParDo.of(new PackageActivityInfo.ParseLine()))
		// Apply a ParDo converting the PackageInfo to a TableRow in
		// the output table
		 .apply(ParDo.of(new DoFn<PackageActivityInfo, TableRow>() {
			@Override
			public void processElement(ProcessContext c) {
				LOG.warn("Element = " + c.element().toString());

				if (c.element().isArrival()) {
					LOG.warn("Exporting");

					c.output(
						new TableRow()
							.set("packageId", c.element().getPackageId())
							.set("location", c.element().getLocation())
							.set("truckId", c.element().getTruckId())
							.set("timestamp", c.element().getTime()));
					}
				}	
		 	}))
		  // Write the Table rows to the output table.  The dataset must already exist
		  // before executing this command.  If you have not created it, use the BigQuery
		  // UI in the Developers Console to create the dataset.
		  //
		  // With the option CREATE_IF_NEEDED, the table will be created if it doesn't
		  // already exist.
		  // Use the BigQuery Query UI to verify your export:
		  // SELECT * FROM partner_training_dataset.package_info LIMIT 5;
		  .apply(BigQueryIO.Write.named("BigQuery-Write")
				.to("google.com:deft-testing-integration:partner_training_dataset.package_info")
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.withSchema(schema));

		p.run();
	}
}
