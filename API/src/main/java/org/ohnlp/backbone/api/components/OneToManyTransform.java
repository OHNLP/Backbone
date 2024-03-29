package org.ohnlp.backbone.api.components;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class OneToManyTransform extends TransformComponent implements SingleInputComponent {

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
        Map<String, PCollection<Row>> collMap = input.getAll();
        return expand(collMap.get(collMap.keySet().toArray(new String[0])[0]));
    }

    @Override
    public Map<String, Schema> calculateOutputSchema(Map<String, Schema> input) {
        if (input.size() != 1) {
            throw new IllegalArgumentException(input.size() + " inputs are provided to " + this.getClass().getSimpleName()
                    + " where exactly 1 is expected");
        }
        return calculateOutputSchema(input.get(input.keySet().toArray(new String[0])[0]));
    }

    public abstract Map<String, Schema> calculateOutputSchema(Schema input);
    public abstract PCollectionRowTuple expand(PCollection<Row> input);
    @Override
    public final List<String> getInputTags() {
        return Collections.singletonList(getInputTag());
    }
    public String getInputTag() {
        return "*";
    }

}
