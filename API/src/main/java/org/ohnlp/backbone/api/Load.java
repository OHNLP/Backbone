package org.ohnlp.backbone.api;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

/**
 * Represents a component that performs the load (output) part of an ETL process
 *
 * It is assumed that data will be transformed into beam {@link Row}s prior to this step
 */
public abstract class Load extends BackbonePipelineComponent<PCollection<Row>, PDone> {
    @Override
    public Class<?> getInputType() {
        return Row.class;
    }
    @Override
    public Class<?> getOutputType() {
        return Void.class;
    }
}
