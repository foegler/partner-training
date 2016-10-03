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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.Mean;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * Keep Truckin Exercise 6
 * 
 * Use Filters and Combine functions to calculate statistics for PCollections.
 */
@SuppressWarnings("serial")
public class Exercise6 {
  private static final Logger LOG = LoggerFactory.getLogger(Exercise6.class);

  // A function to format the output count results.
  public static class FormatOutput extends
      SimpleFunction<KV<String, Long>, String> {
    @Override
    public String apply(KV<String, Long> input) {
      return input.getKey() + ": " + input.getValue();

    }
  }

  // A function to format the output count results.
  public static class GetValues extends
      SimpleFunction<KV<String, Long>, Long> {
    @Override
    public Long apply(KV<String, Long> input) {
      return input.getValue();

    }
  }

  public static void main(String[] args) {
    Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args)
                                                       .withValidation()
                                                       .create());

    String filePath = "gs://deft-foegler/";
    if (p.getOptions().getRunner().getSimpleName()
         .equals("DirectPipelineRunner")) {
      // The location of small test files on your local machine
      filePath = "/Users/foegler/Documents/";
    } else {
      // Your staging location or any other cloud storage location where you
      // will upload files.
      filePath = "gs://deft-foegler/";
    }

    // Read the log lines from file.
    PCollection<KV<String, Long>> counts =
        p.apply(TextIO.Read.from(filePath + "package_log.txt"))
         // Parse the log lines into objects.
         .apply(ParDo.of(new PackageActivityInfo.ParseLine()))
         // Apply a Filter to select the "VIP" packages.
         .apply(Filter.byPredicate(new SerializableFunction<PackageActivityInfo, Boolean>() {
           public Boolean apply(
               PackageActivityInfo s) {
             return s.getPackageId()
                     .startsWith("VIP");
           }
         }))
         // Extract the location as the key for each object.
         .apply(WithKeys.of(new SerializableFunction<PackageActivityInfo, String>() {
           public String apply(
               PackageActivityInfo s) {
             return s.getLocation();
           }
         }))
         // Use the built-in perKey Count transform to count the number of
         // VIP packages for each location.
         .apply(Count.<String, PackageActivityInfo> perKey());

    // Map each key value pair to an output string for writing to file.
    counts.apply(MapElements.via(new FormatOutput()))
          // Log the results to a text file.
          .apply(TextIO.Write.named("WriteVIPCounts").to(filePath + "output"));

    // Extract the values from the key-value pairs.
    counts.apply(MapElements.via(new GetValues()))
          // Calculate the mean number of VIP packages for all locations.
          .apply(Mean.<Long> globally())
          // Log to console / CloudLogging.
          .apply(ParDo.of(new DoFn<Double, Void>() {
            @Override
            public void processElement(ProcessContext c) {
              LOG.info("The Mean number of packages is " + c.element()
                                                            .toString());
            }
          }));

    p.run();
  }
}
