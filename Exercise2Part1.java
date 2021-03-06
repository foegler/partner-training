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
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

/**
 * KeepTruckin Exercise 2 Part 1
 * 
 * This exercise introduces using the TextIO class to read and write to text
 * files.
 * 
 */
public class Exercise2Part1 {
  private static final Logger LOG =
      LoggerFactory.getLogger(Exercise2Part1.class);

  public static void main(String[] args) {
    // Create a Pipeline using from any arguments passed in from the
    // Run Configuration.
    Pipeline p =
        Pipeline.create(PipelineOptionsFactory.fromArgs(args)
                                              .withValidation().create());

    String filePath = "/Users/foegler/Documents/";

    // Use TextIO to read in a log file from disk.
    p.apply(TextIO.Read.from(filePath + "package_log.txt"))
     // Apply a ParDo using the parsing function provided in
     // PackageActivityInfo.
     .apply(ParDo.of(new PackageActivityInfo.ParseLine()))
     // Define a DoFn inline to convert the package info objects to strings.
     .apply(ParDo.of(new DoFn<PackageActivityInfo, String>() {
       @Override
       public void processElement(ProcessContext c) {
         c.output(c.element().toString());
       }
     }))
     // Use TextIO to output the strings to a text file.
     .apply(TextIO.Write.to(filePath + "package_info_out.txt"));
    p.run();
  }
}
