package org.ohnlp.backbone.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.api.config.BackboneConfiguration;
import org.ohnlp.backbone.core.pipeline.ExecutionDAG;

import java.io.IOException;

public class BackboneRunner {
    public static void main(String... args) throws IOException, ComponentInitializationException {
        PipelineOptionsFactory.register(BackbonePipelineOptions.class);
        BackbonePipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).create().as(BackbonePipelineOptions.class);
        Pipeline p = Pipeline.create(options);
        // First read in the config and create an execution plan
        BackboneConfiguration config = new ObjectMapper().readValue(BackboneRunner.class.getResourceAsStream("/configs/" + options.getConfig()), BackboneConfiguration.class);
        ExecutionDAG graph = PipelineBuilder.getPipelineGraph(config);
        graph.planDAG(p);
        // Now run
        p.run().waitUntilFinish();
        // - Done
        System.out.println("Pipeline complete");
        System.exit(0);
    }
}
