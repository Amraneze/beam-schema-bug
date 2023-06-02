package org.apache.beam;

import org.apache.beam.config.DirectPipelineOptions;
import org.apache.beam.config.SchemaBugPipelineOptions;
import org.apache.beam.model.Message;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.transform.LogFn;
import org.apache.beam.transform.ToMessageConverterFn;
import org.apache.beam.transform.ToRow;
import org.joda.time.Duration;

public class MainPipeline {

    private static final String SCHEMA_STEP_NAME = "Schema Step";
    private static final String CONVERT_STEP_NAME = "Convert Step";
    private static final String SCHEMA_BUG_NAME = "beam-schema-bug";

    public static void main(String[] args) {
        SchemaBugPipelineOptions options = getDirectRunnerOptions();
        createPipeline(options);
    }

    private static SchemaBugPipelineOptions getDirectRunnerOptions() {
        var options = PipelineOptionsFactory.create().as(DirectPipelineOptions.class);
        options.setRunner(DirectRunner.class);
        options.setJobName(SCHEMA_BUG_NAME);
        return options;
    }

    private static void createPipeline(SchemaBugPipelineOptions options) {
        var pipeline = Pipeline.create(options);
        var rawMessages = pipeline.apply("Generator", GenerateSequence.from(0).withRate(10, Duration.standardSeconds(10L)));
        var rows = rawMessages.apply(new ToRow());
        rows.apply(Convert.to(Message.class)).apply(ParDo.of(new LogFn(CONVERT_STEP_NAME)));
        rows.apply(ParDo.of(new ToMessageConverterFn())).apply(ParDo.of(new LogFn(SCHEMA_STEP_NAME)));
        pipeline.run();
    }
}
