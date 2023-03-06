package org.ohnlp.backbone.api.components;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.BackbonePipelineComponent;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a component that performs a single input/single output transform
 */
public abstract class OneToOneTransform extends BackbonePipelineComponent<PCollectionRowTuple, PCollectionRowTuple>
        implements HasInputs, HasOutputs {
    @Override
    public Map<String, Schema> calculateOutputSchema(Map<String, Schema> input) {
        if (input.size() != 1) {
            throw new IllegalArgumentException(input.size() + " inputs are provided to " + this.getClass().getSimpleName()
                    + " where exactly 1 is expected");
        }
        Map<String, Schema> ret = new HashMap<>();
        input.forEach((id, schema) -> {
            ret.put(getOutputTags().get(0), calculateOutputSchema(schema));
        });
        return ret;
    }

    @Override
    public final List<String> getInputTags() {
        return Collections.singletonList(getInputTag());
    }

    @Override
    public final List<String> getOutputTags() {
        return Collections.singletonList(getOutputTag());
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
        PCollectionRowTuple ret = PCollectionRowTuple.empty(input.getPipeline());
        input.getAll().forEach((id, coll) -> ret.and(getOutputTags().get(0), expand(coll)));
        return ret;
    }

    public abstract Schema calculateOutputSchema(Schema schema);
    public abstract PCollection<Row> expand(PCollection<Row> input);

    public String getInputTag() {
        return "*";
    }

    public String getOutputTag() {
        return getClass().getSimpleName();
    }
}
