package org.ohnlp.backbone.api.components;

import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.ohnlp.backbone.api.BackbonePipelineComponent;

public abstract class TransformComponent extends BackbonePipelineComponent<PCollectionRowTuple, PCollectionRowTuple> implements HasInputs, HasOutputs {
}
