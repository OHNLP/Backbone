package org.ohnlp.backbone.api.components;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents a component that performs a single input/single output transform
 */
public abstract class OneToOneTransform extends TransformComponent implements SingleInputComponent, SingleOutputComponent {
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
        AtomicReference<PCollectionRowTuple> ret = new AtomicReference<>(PCollectionRowTuple.empty(input.getPipeline()));
        input.getAll().forEach((id, coll) -> ret.set(ret.get().and(getOutputTags().get(0), expand(coll))));
        return ret.get();
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
