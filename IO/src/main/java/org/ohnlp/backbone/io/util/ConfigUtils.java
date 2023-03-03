package org.ohnlp.backbone.io.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.beam.sdk.schemas.Schema;

import java.util.Locale;

public class ConfigUtils {
    public static Schema resolveObjectSchema(JsonNode input) {
        Schema.Builder schemaBuilder = Schema.builder();

        input.fields().forEachRemaining(e -> {
            String fieldName = e.getKey();
            JsonNode typeDef = e.getValue();
            Schema.FieldType type = resolveType(typeDef);
            schemaBuilder.addField(fieldName, type);
        });
        return schemaBuilder.build();
    }

    private static Schema.FieldType resolveType(JsonNode typeDef) {
        Schema.FieldType type;
        if (typeDef.isArray()) {
            ArrayNode nodeAsArr = ((ArrayNode) typeDef);
            if (nodeAsArr.size() != 1) {
                throw new IllegalArgumentException("Array schema types should have exactly one element defining the contents for the array, but " + nodeAsArr.size() + " elements were found");
            }
            type = Schema.FieldType.array(resolveType(nodeAsArr.get(0)));
        } else if (typeDef.isObject()) {
            type = Schema.FieldType.row(resolveObjectSchema(typeDef));
        } else {
            try {
                // TODO add map type support
                type = (Schema.FieldType) Schema.FieldType.class.getDeclaredField(typeDef.asText().toUpperCase(Locale.ROOT)).get(null);
            } catch (IllegalAccessException | NoSuchFieldException ex) {
                throw new RuntimeException(ex);
            }
        }
        return type;
    }
}
