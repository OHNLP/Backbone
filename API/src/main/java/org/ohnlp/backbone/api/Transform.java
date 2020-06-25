package org.ohnlp.backbone.api;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * Represents a component that performs the transform part of an ETL process
 * @param <I> The input type
 * @param <O> The output type
 */
public abstract class Transform<I, O> extends PTransform<PCollection<I>, PCollection<O>> implements BackbonePipelineComponent {


}
