package org.ohnlp.backbone.api.components;

import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.ohnlp.backbone.api.BackbonePipelineComponent;

public abstract class ExtractComponent extends BackbonePipelineComponent<PBegin, PCollectionRowTuple> implements HasOutputs {
}
