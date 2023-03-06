package org.ohnlp.backbone.api.components.legacy.v2;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Transform;
import org.ohnlp.backbone.api.components.OneToOneTransform;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

public class WrappedTransform extends OneToOneTransform {

    private final Transform transform;
    private Schema schema;

    public WrappedTransform(Transform transform) {
        this.transform = transform;
    }

    @Override
    public void init() throws ComponentInitializationException {

    }

    @Override
    public Schema calculateOutputSchema(Schema schema) {
        this.schema = transform.calculateOutputSchema(schema);
        return this.schema;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        PCollection<Row> result = input.apply(transform);
        if (!result.hasSchema()) {
            result.setRowSchema(schema);
        }
        return result;
    }
}
