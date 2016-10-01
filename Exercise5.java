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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.values.KV;

/**
 * Keep Truckin Exercise 5
 * 
 * Keyed operations: WithKey and GroupByKey
 * 
 * Use GroupByKey to count the number of unique VIP packages surfacing at each
 * location.
 * 
 * VIP package ids begin with "VIP".
 */
@SuppressWarnings("serial")
public class Exercise5 {
  private static final Logger LOG = LoggerFactory.getLogger(Exercise5.class);

  // This DoFn operates on the output of a GroupByKey, which is a
  // key-value pair. The value is an Iterable of all values for
  // the key.
  static class FindUniqueVIPPackages extends
          DoFn<KV<String, Iterable<PackageActivityInfo>>, String> {
    @Override
    public void processElement(ProcessContext c) {
      // Create a local set of packages seen at this location
      // to remove duplicate entries.
      HashSet<String> previousPackages = new HashSet<String>();
      for (PackageActivityInfo packageInfo : c.element().getValue()) {
        String currentPackageId = packageInfo.getPackageId();
        // Only add VIP packages to the set.
        if (currentPackageId.startsWith("VIP")) {
          previousPackages.add(currentPackageId);
        }
      }
      c.output(c.element().getKey() + ": " + previousPackages.size());
      // If running on Cloud Dataflow, the log line below
      // would appear in Cloud Logging
      // LOG.info(c.element().getKey());
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
    p.apply(TextIO.Read.from(filePath + "package_log.txt"))
     // Parse the log lines into objects.
     .apply(ParDo.of(new PackageActivityInfo.ParseLine()))
     // Extract the key from each object.
     .apply(WithKeys.of(new SerializableFunction<PackageActivityInfo, String>() {
       public String apply(PackageActivityInfo s) {
         return s.getLocation();
       }
     }))
     // Apply GroupByKey to collect all objects with the same
     // location together.
     .apply(GroupByKey.<String, PackageActivityInfo> create())
     // Count the number of unique VIP packages at each location.
     .apply(ParDo.of(new FindUniqueVIPPackages()))
     // Report the results to file.
     .apply(TextIO.Write.named("WriteVIPCounts").to(
             filePath + "unique_vip_pkgs_count.txt"));
    p.run();
  }
}
