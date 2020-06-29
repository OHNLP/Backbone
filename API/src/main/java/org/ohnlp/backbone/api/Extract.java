package org.ohnlp.backbone.api;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Represents a component that performs the extract (input) part of an ETL pipeline
 *
 * It is expected that implementations will always output in Beam {@link Row} format
 */
public abstract class Extract extends BackbonePipelineComponent<PBegin, PCollection<Row>> {
    @Override
    public Class<?> getInputType() {
        return Void.class;
    }

    @Override
    public Class<?> getOutputType() {
        return Row.class;
    }
}
