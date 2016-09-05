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
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterProcessingTime;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Repeatedly;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import org.joda.time.Duration;
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
 * environment.
 *
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform,
 * you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 * --runner=BlockingDataflowPipelineRunner
 */
public class ExerciseSource {
	private static final Logger LOG = LoggerFactory.getLogger(ExerciseSource.class);
	private static final Duration ALLOWED_LATENESS = Duration.standardMinutes(30);
	private static final Duration EARLY_UPDATE_FREQUENCY = Duration.standardMinutes(1);
	private static final Duration LATE_UPDATE_FREQUENCY = Duration.standardMinutes(2);
	private static final Duration WINDOW_SIZE = Duration.standardMinutes(5);

	public static class UnboundedGenerator
			extends PTransform<PBegin, PCollection<PackageActivityInfo>> {
		@Override
		public PCollection<PackageActivityInfo> apply(PBegin input) {
			return input.apply(Read.from(new GenericUnboundedSource()));
		}
	}

	  private static class ExtractAndSumPackages
      extends PTransform<PCollection<PackageActivityInfo>, PCollection<KV<String, Integer>>> {
	  ExtractAndSumPackages() {}

	  @Override
	  public PCollection<KV<String, Integer>> apply(
	      PCollection<PackageActivityInfo> gameInfo) {
	    return gameInfo
	      .apply(MapElements
	        .via((PackageActivityInfo gInfo) -> KV.of(gInfo.location, 1))
	        .withOutputType(new TypeDescriptor<KV<String, Integer>>() {}))
	      .apply(Sum.<String>integersPerKey());
	    }
	  }
	
	// Extract packages/location
	  public static class UserLeaderBoard
	    extends PTransform<PCollection<PackageActivityInfo>, PCollection<KV<String, Integer>>> {

	    private Duration allowedLateness;
	    private Duration updateFrequency;

	    public UserLeaderBoard(Duration allowedLateness, Duration updateFrequency) {
	      this.allowedLateness = allowedLateness;
	      this.updateFrequency = updateFrequency;
	    }

	    @Override
	    public PCollection<KV<String, Integer>> apply(PCollection<PackageActivityInfo> input) {
	      return input
	          .apply(Window.<PackageActivityInfo>into(new GlobalWindows())
	              .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
	                  .plusDelayOf(updateFrequency)))
	              .accumulatingFiredPanes()
	              .withAllowedLateness(allowedLateness))
	          // TODO(laraschmidt): Can we just use a count here? Possibly not
	          .apply("ExtractUserScore", new ExtractAndSumPackages());
	    }
	  }
	  
	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		DataflowPipelineOptions dataflowOptions= options.as(DataflowPipelineOptions.class);
		dataflowOptions.setStreaming(true);
	    ExerciseOptions eo = dataflowOptions.as(ExerciseOptions.class);
	    eo.setDataset("laradataset");
		Pipeline p = Pipeline.create(eo);

		// Read game events from the unbounded injector.
		PCollection<PackageActivityInfo> results = p.apply(new UnboundedGenerator());
        results.apply(new UserLeaderBoard(ALLOWED_LATENESS, EARLY_UPDATE_FREQUENCY))
        // Write the results to BigQuery.
        .apply(new Output.WriteTriggeredUserScoreSums());
		
		p.run();
	}
}
