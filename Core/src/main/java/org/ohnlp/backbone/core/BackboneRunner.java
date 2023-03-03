package org.ohnlp.backbone.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Transform;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.core.config.BackboneConfiguration;

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
        Schema workingSchema = pipeline.extract.calculateOutputSchema(null);
        PCollection<Row> df = p.apply("Extract-Step-0", pipeline.extract);
        if (workingSchema != null) {
            df = df.setRowSchema(workingSchema).setCoder(RowCoder.of(workingSchema));
        }
        workingSchema = workingSchema == null ? df.getSchema() : workingSchema;
        // - Transform
        int i = 1;
        for (Transform t : pipeline.transforms) {
            workingSchema = t.calculateOutputSchema(workingSchema);
            df = df.apply("Transform-Step-" + i++, t).setRowSchema(workingSchema).setCoder(RowCoder.of(workingSchema));
        }
        // - Load
        pipeline.load.calculateOutputSchema(workingSchema);
        POutput complete = df.apply("Load-Step-" + i++, pipeline.load);
        p.run().waitUntilFinish();
        // - Done
        System.out.println("Pipeline complete");
        System.exit(0);
    }
}
