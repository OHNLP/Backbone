package org.ohnlp.backbone.api.components;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

import java.util.Collections;
import java.util.List;

public abstract class LoadFromOne extends LoadComponent implements SingleInputComponent {

    @Override
    public POutput expand(PCollectionRowTuple input) {
        return expand(input.get(input.getAll().keySet().toArray(new String[0])[0]));
    }

    @Override
    public List<String> getInputTags() {
        return Collections.singletonList("*");
    }

    public abstract POutput expand(PCollection<Row> input);
}
