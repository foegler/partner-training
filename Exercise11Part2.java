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
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keep Truckin Exercise 11 Part 2
 * 
 * Use Windows to group by hour.
 */
@SuppressWarnings("serial")
public class Exercise11Part2 {
	private static final Logger LOG = LoggerFactory.getLogger(Exercise11Part2.class);

	// A function to format the output count results.
	public static class FormatOutput extends DoFn<KV<String, Long>, String>
    	implements DoFn.RequiresWindowAccess {
	   @Override
	   	public void processElement(ProcessContext c) {
				c.output( c.element().getKey() + ": " + c.element().getValue() + " " + ((IntervalWindow) c.window()).start()) ;
			}
		}

	public static void main(String[] args) {
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args)
				.withValidation().create());

		String filePath = "gs://deft-foegler/";
		if (p.getOptions().getRunner().getSimpleName().equals("DirectPipelineRunner")){
			// The location of small test files on your local machine
			filePath = "/Users/foegler/Documents/";
		} else {
			// Your staging location or any other cloud storage location where you will upload files.
			filePath = "gs://deft-foegler/"; 
		}
		
		// Read the log lines from file.
		p.apply(TextIO.Read.from(filePath + "package_log.txt"))
		// Parse the log lines into objects.
		 .apply(ParDo.of(new PackageActivityInfo.ParseLine()))
		 // Since bounded data sources do not contain timestamps, we need to
		 // emit each element from the PCollection with the time as the
		 // timestamp.
		 .apply(ParDo.of(new DoFn<PackageActivityInfo, PackageActivityInfo>() {
		    public void processElement(ProcessContext c) {
	            // Extract the timestamp from log entry we're currently processing.
	            Instant logTimeStamp = new Instant(((PackageActivityInfo) c.element()).getTime().getTime());
	            // Use outputWithTimestamp to emit the log entry with timestamp attached.
	            c.outputWithTimestamp(c.element(), logTimeStamp);
		     }}))
		// Define a hour long window for the data.
		 .apply(Window.<PackageActivityInfo>into(
				 FixedWindows.of(Duration.standardMinutes(60))))
		// Extract the location key from each object.
		 .apply(WithKeys
				.of(new SerializableFunction<PackageActivityInfo, String>() {
					public String apply(PackageActivityInfo s) {
						return s.getLocation();
					}
				}))				 
		// Count the objects from the same hour, per location.
		 .apply(Count.<String, PackageActivityInfo> perKey())
		// Format the output.  Need to use a ParDo since need access
		// to the window time.
		 .apply(ParDo.of(new FormatOutput()))
		// Report the results to file. 
		 .apply(TextIO.Write.named("WritePerHourCounts").to(
				 filePath + "per_hour_per_location_count.txt"));
		p.run();
	}
}
