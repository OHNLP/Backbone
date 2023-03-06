package org.ohnlp.backbone.api.components;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.ohnlp.backbone.api.BackbonePipelineComponent;

import java.util.Map;

public abstract class ExtractToMany extends BackbonePipelineComponent<PBegin, PCollectionRowTuple> implements HasOutputs {

    @Override
    public final Map<String, Schema> calculateOutputSchema(Map<String, Schema> input) {
        return calculateOutputSchema();
    }

    public abstract Map<String, Schema> calculateOutputSchema();
}
