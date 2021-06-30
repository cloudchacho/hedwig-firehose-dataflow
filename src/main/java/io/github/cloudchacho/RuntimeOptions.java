// Copyright 2018 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.github.cloudchacho;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import java.util.List;

public interface RuntimeOptions extends DataflowPipelineOptions {

    @Description("The Cloud Pub/Sub subscriptions to read from. Must be a list of subscription names (e.g. " +
        "hedwig-firehose-dev-user-created-v1). All subscriptions must live in the current project.")
    @Default.InstanceFactory(EmptyStringListFactory.class)
    List<String> getInputSubscriptions();
    void setInputSubscriptions(List<String> value);

    @Description("The Cloud Pub/Sub cross-project subscriptions to read from. Must be a list of subscription names " +
        "and project id joined by ; (e.g. hedwig-firehose-myproject-dev-user-created-v1;myproject). All " +
        "subscriptions must live in the current project.")
    @Default.InstanceFactory(EmptyStringListFactory.class)
    List<String> getInputSubscriptionsCrossProject();
    void setInputSubscriptionsCrossProject(List<String> value);

    @Description("The directory to output files to. Must end with a slash.")
    @Validation.Required
    ValueProvider<String> getOutputDirectory();
    void setOutputDirectory(ValueProvider<String> value);

    @Description("The directory to output temporary files to. Must end with a slash.")
    ValueProvider<String> getUserTempLocation();
    void setUserTempLocation(ValueProvider<String> value);

    @Description("The filename prefix of the files to write to.")
    @Default.String("output")
    @Validation.Required
    ValueProvider<String> getOutputFilenamePrefix();
    void setOutputFilenamePrefix(ValueProvider<String> value);

    @Description("The maximum number of output shards produced when writing.")
    @Default.Integer(1)
    Integer getNumShards();
    void setNumShards(Integer value);

    @Description("The window duration in which data will be written. Defaults to 5m. "
        + "Allowed formats are: "
        + "Ns (for seconds, example: 5s), "
        + "Nm (for minutes, example: 12m), "
        + "Nh (for hours, example: 2h).")
    @Default.String("5m")
    String getWindowDuration();
    void setWindowDuration(String value);
}
