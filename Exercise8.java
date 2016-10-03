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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;

/**
 * Keep Truckin Exercise 9
 * 
 * Use SideInputs and CoGBK.
 */
@SuppressWarnings("serial")
public class Exercise8 {
  private static final Logger LOG = LoggerFactory.getLogger(Exercise8.class);

  // A function to format the output count results.
  private static class FormatOutput extends
      SimpleFunction<KV<String, String[]>, String> {
    @Override
    public String apply(KV<String, String[]> input) {
      StringBuffer output = new StringBuffer();
      output.append(input.getKey());
      output.append(String.join("", Collections.nCopies(10, "*")));
      output.append("\n");
      for (String line : input.getValue()) {
        output.append(line);
        output.append("\n");
      }
      output.append("End " + input.getKey());
      output.append(String.join("", Collections.nCopies(6, "*")));
      output.append("\n");
      LOG.warn("output = " + output.toString());
      return output.toString();
    }
  }
  
  private static class GenerateAsMap extends
      DoFn<String, KV<String, String>> {

    @Override
    public void processElement(ProcessContext c) throws Exception {
      String[] values = c.element().split(" ");
      LOG.warn("adding to map");
      c.output(KV.of(values[0], values[1]));
    }
  }

  static class ProcessCallLogData extends DoFn<KV<String, CoGbkResult>, KV<String, String[]>> {
    private final PCollectionView<Map<String, String>> locationNameMap;

    public ProcessCallLogData(
        PCollectionView<Map<String, String>> locationNameMap) {
      this.locationNameMap = locationNameMap;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      Map<String, String> nameMap = c.sideInput(locationNameMap);

      KV<String, CoGbkResult> e = c.element();
      Iterable<PackageActivityInfo> infos = e.getValue().getAll(infoTag);
      Iterable<Long> calls = e.getValue().getAll(supportTag);
      
      List<String> outLines = new ArrayList<String>();
      // Use the side input to get the full location name.
      outLines.add("Location: " + nameMap.get(c.element().getKey()));

      // There will be only one element in calls since this is the
      // result of a count.
      outLines.add("Total # Support Calls: " + calls.iterator().next());
      
      for (PackageActivityInfo info : infos) {
        outLines.add((info.isArrival() ? "Arrived: " : "Departed: ") + info.getPackageId());
      }
      String[] array = new String[outLines.size()];
      outLines.toArray(array);
      c.output(KV.<String, String[]> of(c.element().getKey(),
                                        array));
    }
  };


  static final TupleTag<PackageActivityInfo> infoTag =
      new TupleTag<PackageActivityInfo>();
  static final TupleTag<Long> supportTag = new TupleTag<Long>();
  
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
    PCollection<KV<String, PackageActivityInfo>> package_info =
        p.apply(TextIO.Read.from(filePath + "package_log.txt"))
         // Parse the log lines into objects.
         .apply(ParDo.of(new PackageActivityInfo.ParseLine()))

         // Extract the location as the key for each object.
         .apply(WithKeys.of(new SerializableFunction<PackageActivityInfo, String>() {
           public String apply(
               PackageActivityInfo s) {
             return s.getLocation();
           }
         }));

    // Read in the support call log. Each line contains a space separated
    // location and timestamp to indicate where/when the support call was made.
    // Count the number of calls per location.
    PCollection<KV<String, Long>> support_calls =
        p.apply(TextIO.Read.from(filePath + "support_call_log.txt"))
         // Parse each line into a KV pair, with location as the key.
         .apply(ParDo.of(new DoFn<String, KV<String, Long>>() {
           @Override
           public void processElement(ProcessContext c) {
             String[] data = c.element().split(" ");
             c.output(KV.<String, Long> of(data[0], Long.parseLong(data[1])));
           }
         }))
         .apply(Count.<String,Long> perKey());
    
    // Input to a CoGroupByKey is a KeyedPCollectionTuple. This is a
    // set of PCollections, where each PCollection is paired with a
    // TupleTag for identification.
    // CoGroupByKeys return a CoGBKResult.
    PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
        KeyedPCollectionTuple.of(infoTag, package_info)
                             .and(supportTag, support_calls)
                             .apply(CoGroupByKey.<String> create());

    // PCollectionViews are used to pass PCollections as side inputs.
    // Create a PCollection view of the data from the location name
    // map file.
    PCollectionView<Map<String, String>> locationSideInput =
        p.apply(TextIO.Read.from(filePath + "location_name_map.txt"))
         .apply(ParDo.of(new GenerateAsMap()))
         .apply(View.<String, String> asMap());

    // Create the output log for each location, using the support call
    // data, the log data, and the side input to map the location name.
    coGbkResultCollection.apply(ParDo.withSideInputs(locationSideInput)
                                     .of(new ProcessCallLogData(
                                         locationSideInput)))
                         // Map each key value pair to an output string for
                         // writing to file.
                         .apply(MapElements.via(new FormatOutput()))
                         // Log the results to a text file.
                         .apply(TextIO.Write.to(filePath + "location_call_output"));

    p.run();
  }
}
