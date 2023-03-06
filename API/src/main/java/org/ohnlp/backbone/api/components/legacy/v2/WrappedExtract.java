package org.ohnlp.backbone.api.components.legacy.v2;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.components.ExtractToOne;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

public class WrappedExtract extends ExtractToOne {

    private final Extract component;

    public WrappedExtract(Extract component) {
        this.component = component;
    }

    @Override
    public void init() throws ComponentInitializationException {

    }

    @Override
    public Schema calculateOutputSchema() {
        return component.calculateOutputSchema(null);
    }

    @Override
    public PCollection<Row> begin(PBegin input) {
        return component.expand(input);
    }
}
