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

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;

import PartnerTraining.Exercise7.FormatOutput;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keep Truckin Exercise 11 Part 1
 * 
 * Use GroupByKey to group by hour.
 */
@SuppressWarnings("serial")
public class Exercise11Part1 {
	private static final Logger LOG = LoggerFactory.getLogger(Exercise11Part1.class);

	// A function to format the output count results.
	public static class FormatOutput extends
			SimpleFunction<KV<Long, Long>, String> {
		@Override
		public String apply(KV<Long, Long> input) {
			Date date = new Date();
			date.setTime(input.getKey());
			return date + ": " + input.getValue();
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
		// Extract the key from each object. The key for each activity
		// is the time rounded to the start of the hour.
		 .apply(WithKeys
					.of(new SerializableFunction<PackageActivityInfo, Long>() {
						public Long apply(PackageActivityInfo s) {
							GregorianCalendar time = new GregorianCalendar();
							time.setTime(s.getTime());
							time.set(Calendar.MINUTE, 0);
							time.set(Calendar.SECOND, 0);
							time.set(Calendar.MILLISECOND, 0);
							return time.getTime().getTime();
						}
					}))
		// Count the objects from the same hour.
		 .apply(Count.<Long, PackageActivityInfo> perKey())
		// Format the output.
		 .apply(MapElements.via(new FormatOutput()))
		// Report the results to file. 
		 .apply(TextIO.Write.named("WritePerHourCounts").to(
				 filePath + "per_hour_count.txt"));
		p.run();
	}
}
