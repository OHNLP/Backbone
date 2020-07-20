package org.ohnlp.backbone.api;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Represents a component that performs the transform part of an ETL process
 */
public abstract class Transform extends BackbonePipelineComponent<PCollection<Row>, PCollection<Row>> {

}
