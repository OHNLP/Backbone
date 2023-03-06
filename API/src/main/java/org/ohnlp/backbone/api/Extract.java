package org.ohnlp.backbone.api;

import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

/**
 * Represents a component that performs the extract (input) part of an ETL pipeline
 *
 * It is expected that implementations will always output in Beam {@link Row} format
 */
public abstract class Extract extends BackbonePipelineComponent<PBegin, PCollectionRowTuple> implements HasOutputs {
}
