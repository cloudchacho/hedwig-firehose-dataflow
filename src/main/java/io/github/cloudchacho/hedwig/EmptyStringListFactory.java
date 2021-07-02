package io.github.cloudchacho.hedwig;

import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;

import java.util.ArrayList;
import java.util.List;

public class EmptyStringListFactory implements DefaultValueFactory<List<String>> {
    public List<String> create(PipelineOptions options) {
        return new ArrayList<>();
    }
}
