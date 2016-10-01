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
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

/**
 * KeepTruckin Exercise 2 Part 2
 * 
 * This exercise introduces coders and using the AvroIO class to write to text
 * files.
 * 
 */
public class Exercise2Part2 {
  public static void main(String[] args) {
    Pipeline p =
        Pipeline.create(PipelineOptionsFactory.fromArgs(args)
                                              .withValidation().create());

    String filePath = "/Users/foegler/Documents/";

    // Use TextIO to read in a log file from disk.
    p.apply(TextIO.Read.from(filePath + "package_log.txt"))
     // Apply a ParDo using the parsing function provided in
     // PackageActivityInfo.
     .apply(ParDo.of(new PackageActivityInfo.ParseLine()))
     // Write to a text file using the StringDelegateCoder, which
     // uses the toString method defined on the class to serialize
     // the objects.
     /*
      * .apply(TextIO.Write.withCoder(
      * StringDelegateCoder.of(PackageActivityInfo.class)).to( filePath +
      * "package_info_out.txt"));
      */
     // Write to a text file using the AvroCoder, to serialize
     // the objects.
     /*
      * .apply(TextIO.Write.withCoder(
      * AvroCoder.of(PackageActivityInfo.class)).to( filePath +
      * "package_info_out.txt"));
      */
     // Write to a text file using the AvroIO transform to serialize
     // the objects.
     .apply(AvroIO.Write.withSchema(PackageActivityInfo.class)
                        .to(
                            filePath + "package_info_out.txt"));
    p.run();
  }
}
