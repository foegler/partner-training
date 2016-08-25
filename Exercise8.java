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
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.values.KV;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO - ADD DESCRIPTION
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
public class Exercise8 {
	private static final Logger LOG = LoggerFactory.getLogger(Exercise8.class);

	// A function to format the output count results.
	public static class FormatOutput extends
			SimpleFunction<KV<String, Long>, String> {
		@Override
		public String apply(KV<String, Long> input) {
			return input.getKey() + ": " + input.getValue();

		}
	}

	public static void main(String[] args) {
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args)
				.withValidation().create());

		// p.apply(TextIO.Read.from("gs://deft-foegler/package_log.txt"))
		p.apply(TextIO.Read.from("/Users/foegler/Downloads/package_log.txt"))
		 .apply(ParDo.of(new PackageActivityInfo.ParseLine()))
		 .apply(Filter
				.byPredicate(new SerializableFunction<PackageActivityInfo, Boolean>() {
					public Boolean apply(PackageActivityInfo s) {
						return s.getPackageId().startsWith("VIP");
					}
				}))
		 .apply(WithKeys
				.of(new SerializableFunction<PackageActivityInfo, String>() {
					public String apply(PackageActivityInfo s) {
						return s.getLocation();
					}
				}))
		 .apply(Count.<String, PackageActivityInfo> perKey())
		 .apply(MapElements.via(new FormatOutput()))
		 .apply(TextIO.Write.named("WriteVIPCounts").to(
				"/Users/foegler/Downloads/output"));
		p.run();
	}
}
