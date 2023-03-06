package org.ohnlp.backbone.api.components;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class ExtractToOne extends ExtractComponent implements SingleOutputComponent {

    @Override
    public final PCollectionRowTuple expand(PBegin input) {
        return PCollectionRowTuple.of(getOutputTags().get(0), begin(input));
    }

    @Override
    public List<String> getOutputTags() {
        return Collections.singletonList(getClass().getSimpleName());
    }

    @Override
    public final Map<String, Schema> calculateOutputSchema(Map<String, Schema> input) {
        return Collections.singletonMap(getOutputTags().get(0), calculateOutputSchema());
    }

    public abstract Schema calculateOutputSchema();
    public abstract PCollection<Row> begin(PBegin input);
}
