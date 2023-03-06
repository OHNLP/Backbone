package org.ohnlp.backbone.api.components;

import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.POutput;
import org.ohnlp.backbone.api.BackbonePipelineComponent;

public abstract class LoadComponent extends BackbonePipelineComponent<PCollectionRowTuple, POutput> implements HasInputs {
}
