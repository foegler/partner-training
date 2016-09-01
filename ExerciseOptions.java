package dataflow;

import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.Validation;

public interface ExerciseOptions extends PipelineOptions {
  @Description("BigQuery Dataset to write tables to. Must already exist.")
  @Validation.Required
  String getDataset();
  void setDataset(String value);
}