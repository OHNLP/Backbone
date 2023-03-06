package org.ohnlp.backbone.api.components;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class ManyToOneTransform extends TransformComponent implements SingleOutputComponent {

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
        PCollectionRowTuple output = PCollectionRowTuple.empty(input.getPipeline());
        output = output.and(getOutputTags().get(0), expand(input.getAll()));
        return output;
    }

    @Override
    public Map<String, Schema> calculateOutputSchema(Map<String, Schema> input) {
        return Collections.singletonMap(getOutputTags().get(0), getOutputSchema(input));
    }

    @Override
    public List<String> getOutputTags() {
        return Collections.singletonList(this.getClass().getSimpleName());
    }

    public abstract PCollection<Row> expand(Map<String, PCollection<Row>> input);

    public abstract Schema getOutputSchema(Map<String, Schema> input);
}
