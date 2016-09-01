/*
 * Copyright (C) 2016 Google Inc.
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

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Helpers for writing output
 */
public class Output {

  private static final DateTimeFormatter DATE_TIME_FMT =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
          .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));

  private static class Base<InputT> extends PTransform<PCollection<InputT>, PDone> {

    private final String tableName;
    protected final Map<String, WriteToBigQuery.FieldInfo<InputT>> config = new HashMap<>();

    public Base(String tableName) {
      this.tableName = tableName;
    }

    @Override
    public PDone apply(PCollection<InputT> input) {
      return input.apply(new WriteToBigQuery<InputT>(tableName, config));
    }
  }

  /**
   * Writes to the {@code user_score} table the following columns:
   *   - {@code user} from the string key
   *   - {@code total_score} from the integer value
   */
  public static class WriteUserScoreSums extends Base<KV<String, Integer>> {
    public WriteUserScoreSums() {
      this("user_score");
    }
    protected WriteUserScoreSums(String tableName) {
      super(tableName);
      config.put("user",
          new WriteToBigQuery.FieldInfo<KV<String, Integer>>("STRING", c -> c.element().getKey()));
      config.put("total_score",
          new WriteToBigQuery.FieldInfo<KV<String, Integer>>("INTEGER", c -> c.element().getValue()));

    }
  }

  /**
   * Writes to the {@code hourly_team_score} table the following columns:
   *   - {@code team} from the string key
   *   - {@code total_score} from the integer value
   *   - {@code window_start} from the start time of the window
   */
  public static class WriteHourlyTeamScore extends Base<KV<String, Integer>> {
    public WriteHourlyTeamScore() {
      this("hourly_team_score");
    }

    protected WriteHourlyTeamScore(String tableName) {
      super(tableName);
      config.put("team",
          new WriteToBigQuery.FieldInfo<KV<String, Integer>>("STRING", c -> c.element().getKey()));
      config.put("total_score",
          new WriteToBigQuery.FieldInfo<KV<String, Integer>>("INTEGER", c -> c.element().getValue()));
      config.put("window_start",
          new WriteToBigQuery.FieldInfo<KV<String, Integer>>("STRING",
              c -> { IntervalWindow w = (IntervalWindow) c.window();
              return DATE_TIME_FMT.print(w.start()); }));
    }
  }

  /**
   * Writes to the {@code triggered_user_score} table the following columns:
   *   - {@code user} from the string key
   *   - {@code total_score} from the integer value
   *   - {@code processing_time} the time at which the row was written
   */
  public static class WriteTriggeredUserScoreSums extends WriteUserScoreSums {
    public WriteTriggeredUserScoreSums() {
      super("triggered_user_score");
      config.put("processing_time", new WriteToBigQuery.FieldInfo<KV<String, Integer>>(
              "STRING", c -> DATE_TIME_FMT.print(Instant.now())));
    }
  }

  /**
   * Writes to the {@code triggered_team_score} table the following columns:
   *   - {@code team} from the string key
   *   - {@code total_score} from the integer value
   *   - {@code window_start} from the start time of the window
   *   - {@code processing_time} the time at which the row was written
   *   - {@code timing} a string describing whether the row is early, on-time, or late
   */
  public static class WriteTriggeredTeamScore extends WriteHourlyTeamScore {
    public WriteTriggeredTeamScore() {
      super("triggered_team_score");
      config.put("processing_time", new WriteToBigQuery.FieldInfo<KV<String, Integer>>(
          "STRING", c -> DATE_TIME_FMT.print(Instant.now())));
      config.put("timing", new WriteToBigQuery.FieldInfo<KV<String, Integer>>(
          "STRING", c -> c.pane().getTiming().toString()));
    }
  }
}