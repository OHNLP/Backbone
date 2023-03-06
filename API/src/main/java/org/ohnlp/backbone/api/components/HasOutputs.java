package org.ohnlp.backbone.api.components;

import org.apache.beam.sdk.schemas.Schema;

import java.util.List;
import java.util.Map;

public interface HasOutputs {
    List<String> getOutputTags();
    Map<String, Schema> calculateOutputSchema(Map<String, Schema> input);
}
