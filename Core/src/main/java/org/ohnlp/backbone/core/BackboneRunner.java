package org.ohnlp.backbone.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.codehaus.jackson.map.ObjectMapper;
import org.ohnlp.backbone.api.Transform;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.core.config.BackboneConfiguration;

import java.io.File;
import java.io.IOException;

public class BackboneRunner {
    public static void main(String... args) throws IOException, ComponentInitializationException {
        PipelineOptionsFactory.register(BackbonePipelineOptions.class);
        BackbonePipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).create().as(BackbonePipelineOptions.class);
        Pipeline p = Pipeline.create(options);
        // First read in the config and create an execution plan
        BackboneConfiguration config = new ObjectMapper().readValue(BackboneRunner.class.getResourceAsStream("/configs/" + options.getConfig()), BackboneConfiguration.class);
        PipelineBuilder.BackboneETLPipeline pipeline = PipelineBuilder.buildETLPipelineFromConfig(config);
        // Now run in sequence
        // - Extract
        PCollection<Row> df = p.apply("Extract-Step-0", pipeline.extract);
        // - Transform
        int i = 1;
        for (Transform t : pipeline.transforms) {
            df = df.apply("Transform-Step-" + i++, t);
        }
        // - Load
        PDone complete = df.apply("Load-Step-" + i++, pipeline.load);
    }
}
