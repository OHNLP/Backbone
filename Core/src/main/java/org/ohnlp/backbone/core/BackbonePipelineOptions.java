package org.ohnlp.backbone.core;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface BackbonePipelineOptions extends PipelineOptions {
    @Description("The backbone configuration to use")
    String getConfig();
    void setConfig(String config);
}
