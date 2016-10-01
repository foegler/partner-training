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
import com.google.cloud.dataflow.sdk.coders.StringDelegateCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;

/**
 * KeepTruckin Exercise 3
 * 
 * Run Exercise 2 Part 3 on Cloud Dataflow service.
 * 
 * The only change required to the file is to use a Google Storage path for the
 * input and output files rather than a local path.
 * 
 * Externally, this means any input files must have been up uploaded to Google
 * Storage.
 * 
 * Your project id and a Google Storage staging location must be set in the
 * Dataflow Run Configurations.
 * 
 * Use the BlockingDataflowPipelineRunner (under Datflow Run Configurations) to
 * execute the Dataflow job on Cloud Dataflow.
 * 
 * To get to the Dataflow Run Configurations, right click on this file in the
 * Project Explorer window. Run As -> Dataflow Pipeline.
 * 
 */
public class Exercise3 {
  // These are the "tags" or names to be used to identify the primary and
  // side output PCollections.
  final static TupleTag<PackageActivityInfo> packageObjects =
      new TupleTag<PackageActivityInfo>() {
      };
  final static TupleTag<String> errorLogs = new TupleTag<String>() {
  };

  private static final Logger LOG = LoggerFactory
                                                 .getLogger(Exercise3.class);

  // ParseLine class copied from PackageActivityInfo.java and modified to
  // produce a side output of the error log lines.
  static class ParseLine extends DoFn<String, PackageActivityInfo> {
    private final Aggregator<Long, Long> invalidLines =
        createAggregator(
                         "invalidLogLines", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c) {
      String logLine = c.element();
      PackageActivityInfo info = PackageActivityInfo.Parse(logLine);
      if (info == null) {
        invalidLines.addValue(1L);
        // Output the unparsable log line to the side output, identifying
        // the side using a tuple tag.
        c.sideOutput(errorLogs, logLine);
      } else {
        // Output the parsed object to the primary output PCollection.
        c.output(info);
      }
    }
  }

  public static void main(String[] args) {
    Pipeline p =
        Pipeline.create(PipelineOptionsFactory.fromArgs(args)
                                              .withValidation().create());

    String filePath = "gs://deft-foegler/";

    // Use .withOutputTags to pass TupleTags to the ParDo. The first
    // argument is the tag for the primary outputPCollection. The second
    // is a list of all side output TupleTags.
    //
    // The output of a ParDo with side outputs is a PCollectionTuple.
    // Capture the PCollectionTuple in a local variable and use 'get'
    // to retrieve the individual PCollecitons from the output.
    PCollectionTuple results =
        p.apply(
                TextIO.Read.from(filePath + "package_log.txt"))
         .apply(ParDo.withOutputTags(packageObjects,
                                     TupleTagList.of(errorLogs))
                     .of(new ParseLine()));

    // Retrieve the primary output and write to file as before.
    results.get(packageObjects)
           .apply(
                  TextIO.Write.withCoder(
                                         StringDelegateCoder.of(PackageActivityInfo.class))
                              .to(filePath + "output/package_out.txt"));

    // Retrieve the side output and write to a separate file.
    results.get(errorLogs)
           .apply(
                  TextIO.Write.to(filePath + "output/package_bad_lines.txt"));
    p.run();
  }
}
