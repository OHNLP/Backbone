package org.ohnlp.backbone.api.components;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.POutput;
import org.ohnlp.backbone.api.BackbonePipelineComponent;

import java.util.Map;

public abstract class LoadComponent extends BackbonePipelineComponent<PCollectionRowTuple, POutput>
        implements HasInputs, SchemaInitializable {
    private boolean schemaInit = false;
    private Map<String, Schema> schema;

    public final boolean isSchemaInit() {
        return schemaInit;
    }

    public final void setSchemaInit() {
        this.schemaInit = true;
    }

    public final void setComponentSchema(Map<String, Schema> schema) {
        this.schema = schema;
    }

    public final Map<String, Schema> getComponentSchema() {
        if (!isSchemaInit()) {
            throw new IllegalStateException("getComponentSchema called without schema being initialized");
        }
        return this.schema;
    }

    @Override
    public Schema getRequiredColumns(String inputTag) {
        return null;
    }

    @Override
    public void validate() { // Do nothing by default
    }
}
