package org.ohnlp.backbone.io.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.schemas.Schema;

import java.util.Locale;
import java.util.Map;

public class ConfigUtils {
    public static Schema resolveSchemaFromConfigJSON(JsonNode input) {
        Schema.Builder schemaBuilder = Schema.builder();

        input.fields().forEachRemaining(e -> {
            String fieldName = e.getKey();
            Schema.FieldType type;
            try {
                type = (Schema.FieldType) Schema.FieldType.class.getDeclaredField(e.getValue().asText().toUpperCase(Locale.ROOT)).get(null);
            } catch (IllegalAccessException | NoSuchFieldException ex) {
                throw new RuntimeException(ex);
            }
            schemaBuilder.addField(fieldName, type);
        });
        return schemaBuilder.build();
    }
}
