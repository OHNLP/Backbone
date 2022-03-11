package org.ohnlp.backbone.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Transform;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.core.coders.RowToByteArrCoder;
import org.ohnlp.backbone.core.config.BackboneConfiguration;

import java.io.IOException;

public class BackboneRunner {
    public static void main(String... args) throws IOException, ComponentInitializationException {
        PipelineOptionsFactory.register(BackbonePipelineOptions.class);
        BackbonePipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).create().as(BackbonePipelineOptions.class);
        Pipeline p = Pipeline.create(options);
        p.getCoderRegistry().registerCoderForClass(Row.class, new RowToByteArrCoder());
        // First read in the config and create an execution plan
        BackboneConfiguration config = new ObjectMapper().readValue(BackboneRunner.class.getResourceAsStream("/configs/" + options.getConfig()), BackboneConfiguration.class);
        PipelineBuilder.BackboneETLPipeline pipeline = PipelineBuilder.buildETLPipelineFromConfig(config);
        // Now run in sequence
        // - Extract
        PCollection<Row> df = p.apply("Extract-Step-0", pipeline.extract).setCoder(new RowToByteArrCoder());
        // - Transform
        int i = 1;
        for (Transform t : pipeline.transforms) {
            df = df.apply("Transform-Step-" + i++, t).setCoder(new RowToByteArrCoder());
        }
        // - Load
        PDone complete = df.apply("Load-Step-" + i++, pipeline.load);
        p.run().waitUntilFinish();
        // - Done
        System.out.println("Pipeline complete");
        System.exit(0);
    }
}
