package org.ohnlp.backbone.api.components;

import org.apache.beam.sdk.values.*;
import org.ohnlp.backbone.api.BackbonePipelineComponent;

import java.util.Collections;
import java.util.List;

public abstract class LoadFromOne extends BackbonePipelineComponent<PCollectionRowTuple, POutput> implements HasInputs{

    @Override
    public POutput expand(PCollectionRowTuple input) {
        PCollectionRowTuple output = PCollectionRowTuple.empty(input.getPipeline());
        return expand(output.get(output.getAll().keySet().toArray(new String[0])[0]));
    }

    @Override
    public List<String> getInputTags() {
        return Collections.singletonList("*");
    }

    public abstract POutput expand(PCollection<Row> input);
}
