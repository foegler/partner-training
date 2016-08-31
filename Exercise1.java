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
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KeepTruckin Exercise 1
 * 
 * This is a simple exercise designed to introduce the construction of a
 * Dataflow Pipeline.
 * 
 * This class requires PackageActivityInfo.java
 */
public class Exercise1 {
	private static final Logger LOG = LoggerFactory.getLogger(Exercise1.class);

	public static void main(String[] args) {
		// Create a Pipeline using from any arguments passed in from the
		// Run Configuration.
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args)
				.withValidation().create());

		// Create.of generates a PCollection of strings, one per log line,
		// using the small set of log lines contained in the array MINI_LOG.
		p.apply(Create.of(PackageActivityInfo.MINI_LOG))
		// Apply a ParDo using the parsing function provided in PackageActivityInfo.	
		 .apply(ParDo.of(new PackageActivityInfo.ParseLine()))
		// Define a DoFn inline to log the package info to the console. 
		 .apply(ParDo.of(new DoFn<PackageActivityInfo, Void>() {
					@Override
					public void processElement(ProcessContext c) {
						LOG.info(c.element().toString());
					}
				}));

		p.run();
	}
}
