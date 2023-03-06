package org.ohnlp.backbone.api.components;

import org.apache.beam.sdk.schemas.Schema;

import java.util.Map;

public interface SchemaInitializable {
    boolean isSchemaInit();
    void setSchemaInit();
    void setComponentSchema(Map<String, Schema> schema);
    Map<String, Schema> getComponentSchema();
}
