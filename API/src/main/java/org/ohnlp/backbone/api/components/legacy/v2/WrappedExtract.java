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
    private Schema schema;

    public WrappedExtract(Extract component) {
        this.component = component;
    }

    @Override
    public void init() throws ComponentInitializationException {

    }

    @Override
    public Schema calculateOutputSchema() {
        this.schema = component.calculateOutputSchema(null);
        return this.schema;
    }

    @Override
    public PCollection<Row> begin(PBegin input) {
        PCollection<Row> result = component.expand(input);
        if (!result.hasSchema()) {
            result.setRowSchema(this.schema);
        }
        return result;
    }
}
