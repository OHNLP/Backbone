package org.ohnlp.backbone.api.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.beam.sdk.schemas.Schema;

import java.util.Locale;

public class SchemaConfigUtils {
    public static Schema jsonToSchema(JsonNode input) {
        Schema.Builder schemaBuilder = Schema.builder();

        input.fields().forEachRemaining(e -> {
            String fieldName = e.getKey();
            JsonNode typeDef = e.getValue();
            Schema.FieldType type = resolveType(typeDef);
            schemaBuilder.addNullableField(fieldName, type);
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
            type = Schema.FieldType.row(jsonToSchema(typeDef));
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

    public static JsonNode schemaToJSON(Schema input) {
        ObjectNode ret = JsonNodeFactory.instance.objectNode();
        input.getFields().forEach(f -> {
            String key = f.getName();
            fieldTypeToJSON(f.getType());
        });
        return ret;
    }

    private static JsonNode fieldTypeToJSON(Schema.FieldType type) {
        if (type.getTypeName().isCollectionType()) {
            ArrayNode ret = JsonNodeFactory.instance.arrayNode();
            JsonNode contents = fieldTypeToJSON(type.getCollectionElementType());
            if (contents != null) {
                ret.add(contents);
            }
            return ret;
        } else if (type.getTypeName().isCompositeType()) {
            return schemaToJSON(type.getRowSchema());
        } else {
            return JsonNodeFactory.instance.textNode(type.getTypeName().name());
        }
    }
}
