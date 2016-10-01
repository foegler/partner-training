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
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.values.KV;

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
 * environment. In Eclipse, this corresponds to the existing 'LOCAL' run
 * configuration.
 *
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform,
 * you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 * --runner=BlockingDataflowPipelineRunner In Eclipse, you can just modify the
 * existing 'SERVICE' run configuration.
 */
@SuppressWarnings("serial")
public class Exercise18 {
	private static final Logger LOG = LoggerFactory.getLogger(Exercise18.class);

	static class ParseLine extends DoFn<String, PackageActivityInfo> {
		private final Aggregator<Long, Long> invalidLines = createAggregator(
				"invalidLogLines", new Sum.SumLongFn());

		@Override
		public void processElement(ProcessContext c) {
			String logLine = c.element();
			LOG.info("Parsing log line: " + logLine);
			PackageActivityInfo info = PackageActivityInfo.Parse(logLine);
			if (info == null) {
				invalidLines.addValue(1L);
			} else {
				c.output(info);
			}
		}
	}

	static class PrintPackagesPerTruck
			extends DoFn<KV<Integer, Iterable<PackageActivityInfo>>, String> {
		@Override
		public void processElement(ProcessContext c) {
			String allPackages = "";
			for (PackageActivityInfo packageInfo : c.element().getValue()) {
				allPackages += packageInfo.getPackageId() + ",  ";
			}
			c.output(c.element().getKey() + ": " + allPackages);
		}
	}

	public static void main(String[] args) {
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args)
				.withValidation().create());

		p.apply(TextIO.Read.from("gs://clouddfe-laraschmidt/package_log.txt"))
				.apply(ParDo.of(new ParseLine()))
				.apply(WithKeys.of(new SerializableFunction<PackageActivityInfo, Integer>() {
					public Integer apply(PackageActivityInfo s) {
						return s.getTruckId();
					}
				})).setCoder(KvCoder.of(VarIntCoder.of(), AvroCoder.of(PackageActivityInfo.class)))
				.apply(GroupByKey.<Integer, PackageActivityInfo>create())
				.apply(ParDo.of(new PrintPackagesPerTruck()))
				.apply(TextIO.Write.named("WritePackagePerId")
						.to("gs://clouddfe-laraschmidt/packages_per_truck.txt"));
		p.run();
	}
}
