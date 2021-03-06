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
 * KeepTruckin Exercise 2 Part 3
 * 
 * This exercise also introduces the concept of "side outputs". Side outputs
 * allow you to output multiple pCollections from the application of a single
 * ParDo. In this example, a ParDo with a primary output of the parsed log data
 * also produces a side output of the unparsable log lines for error reporting.
 * 
 */
public class Exercise2Part3 {
  // These are the "tags" or names to be used to identify the primary and
  // side output PCollections.
  final static TupleTag<PackageActivityInfo> packageObjects =
      new TupleTag<PackageActivityInfo>() {
      };
  final static TupleTag<String> errorLogs = new TupleTag<String>() {
  };

  private static final Logger LOG =
      LoggerFactory
                   .getLogger(Exercise2Part3.class);

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

    String filePath = "/Users/foegler/Documents/";

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
                              .to(filePath + "package_out.txt"));

    // Retrieve the side output and write to a separate file.
    results.get(errorLogs)
           .apply(
                  TextIO.Write.to(filePath + "package_bad_lines.txt"));
    p.run();
  }
}
