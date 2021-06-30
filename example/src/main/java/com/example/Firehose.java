package com.example;

import com.example.hedwig_examples.Schema;
import org.apache.beam.sdk.PipelineResult;

import java.util.List;

public class Firehose {
    public static void main(String[] args) throws IllegalArgumentException {
        PipelineResult result = io.github.cloudchacho.Firehose.run(args, List.of(
            Schema.UserCreatedV1.getDefaultInstance(),
            Schema.UserUpdatedV1.getDefaultInstance()
        ));
        // ignore result?
    }
}
