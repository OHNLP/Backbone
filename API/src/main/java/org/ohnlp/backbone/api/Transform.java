package org.ohnlp.backbone.api;

import org.apache.beam.sdk.values.PCollection;

/**
 * Represents a component that performs the transform part of an ETL process
 * @param <I> The input type
 * @param <O> The output type
 */
public abstract class Transform<I, O> extends BackbonePipelineComponent<PCollection<I>, PCollection<O>> {

}
