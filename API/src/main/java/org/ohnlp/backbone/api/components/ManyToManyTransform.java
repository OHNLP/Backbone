package org.ohnlp.backbone.api.components;

import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.ohnlp.backbone.api.BackbonePipelineComponent;

public abstract class ManyToManyTransform extends BackbonePipelineComponent<PCollectionRowTuple, PCollectionRowTuple>
        implements HasInputs, HasOutputs {
}
