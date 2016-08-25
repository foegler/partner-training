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
 * A starter example for writing Google Cloud Dataflow programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectPipelineRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=BlockingDataflowPipelineRunner
 */
public class Exercise2 {
  private static final Logger LOG = LoggerFactory.getLogger(Exercise2.class);

  
	static class ParseLine extends DoFn<String, PackageActivityInfo> {
		private final Aggregator<Long, Long> invalidLines = createAggregator("invalidLogLines", new Sum.SumLongFn());

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
	
  public static void main(String[] args) {
    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).withValidation().create());

		p.apply(Create.of("0, AN, 1467394122, 423, 372A3SZ4J98",
				          "0, AN, 1467394122, 423##############",
				          "404 - broken message",
				          "0, AN, 1467394122, 423, 372A3SZ4J98"))
	       .apply(ParDo.of(new ParseLine()))
    .apply(ParDo.of(new DoFn<PackageActivityInfo, Void>() {
      @Override
      public void processElement(ProcessContext c)  {
        LOG.info(c.element().toString());
      }
    }));

    p.run();
  }
}
