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
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Partition;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.StringDelegateCoder;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Keep Truckin Exercise 10
 * 
 * Read from two different sources.
 * 
 * In this exercise, you will generate two output files. One which has
 * information about packages shipped from newer truck ids (which are greater
 * than 200) and older truck ids.
 * 
 * You will also process from an additional source, an old Keep Truckin log
 * format that our older data is stored in.
 * TODO(laraschmidt): Make sure this runs after cleanups
 */

public class Exercise7 {
	private static final Logger LOG = LoggerFactory.getLogger(Exercise7.class);

	// A DoFn which processes each line of the old log into PackageActivityInfo.
	// This
	// is similar to normal parsing except we use the old parsing function
	// instead.
	static class ParseOldLines extends DoFn<String, PackageActivityInfo> {
		@Override
		public void processElement(ProcessContext c) {
			String logLine = c.element();
			PackageActivityInfo info = PackageActivityInfo.ParseOld(logLine);
			if (info != null) { // Just ignore errors for this exercise
				c.output(info);
			}
		}
	}

	// A Parition which partitions the results into newer and older trucks based
	// on ID.
	// We know that num_partitions will be 2, since we are calling it.
	@SuppressWarnings("serial")
	static class TruckIdPartition implements Partition.PartitionFn<PackageActivityInfo> {

		// For each package activity info, specifies which partition to place it
		// into.
		public int partitionFor(PackageActivityInfo pack, int num_partitions) {
			return pack.getTruckId() < 200 ? 0 : 1;
		}
	}

	public static void main(String[] args) {
		Pipeline p = Pipeline
				.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

		String filePath = "gs://deft-foegler/";
		if (p.getOptions().getRunner().getSimpleName().equals("DirectPipelineRunner")) {
			// The location of small test files on your local machine
			filePath = "/Users/foegler/Documents/";
		} else {
			// Your staging location or any other cloud storage location where
			// you will upload files.
			filePath = "gs://deft-foegler/";
		}

		// From the normal log, parse the log line into PackageActiviyInfo
		// objects
		PCollection<PackageActivityInfo> input = p
				.apply(TextIO.Read.from(filePath + "package_log.txt"))
				.apply(ParDo.of(new PackageActivityInfo.ParseLine()));

		// From the older log, parse the log line using the older parsing
		// function.
		PCollection<PackageActivityInfo> old_input = p
				.apply(TextIO.Read.from(filePath + "package_log_old.txt"))
				.apply(ParDo.of(new ParseOldLines()));

		// Flatten the sources together into one input.
		PCollection<PackageActivityInfo> packages = PCollectionList.of(input).and(old_input)
				.apply(Flatten.<PackageActivityInfo>pCollections());

		// Apply the partition from above.
		PCollectionList<PackageActivityInfo> truck_lists = packages
				.apply(Partition.of(2, new TruckIdPartition()));

		// TODO(laraschmidt): Fix this once you figure out how array init works in java...
		ArrayList<String> output_files = new ArrayList<>();
		output_files.add("old_truck_packages.txt");
		output_files.add("new_truck_packages.txt");
		
		// Go through each resulting piece of the partition and write to a file.
		for (int i = 0; i < 2; i++) {
			truck_lists.get(i)
					.apply(TextIO.Write.withCoder(StringDelegateCoder.of(PackageActivityInfo.class))
							.to(filePath + output_files.get(i)));

		}
		p.run();
	}
}
