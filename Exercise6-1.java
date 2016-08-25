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

import java.util.HashSet;

import com.google.cloud.dataflow.sdk.Pipeline;
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
public class Exercise6 {
	private static final Logger LOG = LoggerFactory.getLogger(Exercise6.class);

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

	static class FindUniqueVIPPackages extends
			DoFn<KV<String, Iterable<PackageActivityInfo>>, String> {
		@Override
		public void processElement(ProcessContext c) {
			HashSet<String> previousPackages = new HashSet<String>();
			int vipPackageCount = 0;
			for (PackageActivityInfo packageInfo : c.element().getValue()) {
				String currentPackageId = packageInfo.getPackageId();
				if (currentPackageId.startsWith("VIP")) {
					if (previousPackages.contains(currentPackageId)) {
						// Ignore. We've already seen this one.
					} else {
						vipPackageCount++;
						previousPackages.add(currentPackageId);
					}
				}
			}
			c.output(c.element().getKey() + ": " + vipPackageCount);
			// If running in the Cloud, the log lines below
			// would appear in Cloud Logging
			// LOG.info(c.element().getKey());
			// LOG.info(c.element().getValue().toString());
		}
	}

	public static void main(String[] args) {
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args)
				.withValidation().create());

		 p.apply(TextIO.Read.from("gs://deft-foegler/package_log.txt"))
		//p.apply(TextIO.Read.from("/Users/foegler/Downloads/package_log.txt"))
				.apply(ParDo.of(new ParseLine()))
				.apply(WithKeys
						.of(new SerializableFunction<PackageActivityInfo, String>() {
							public String apply(PackageActivityInfo s) {
								return s.getLocation();
							}
						}))
				.apply(GroupByKey.<String, PackageActivityInfo> create())
				.apply(ParDo.of(new FindUniqueVIPPackages()))
				.apply(TextIO.Write.named("WriteVIPCounts").to(
						//"/Users/foegler/Downloads/output"));
						"gs://deft-foegler/output"));
		p.run();
	}
}
