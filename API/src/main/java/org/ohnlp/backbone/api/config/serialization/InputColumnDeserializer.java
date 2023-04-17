package org.ohnlp.backbone.api.config.serialization;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.ohnlp.backbone.api.config.InputColumn;

import java.io.IOException;

public class InputColumnDeserializer extends StdDeserializer<InputColumn> {
    protected InputColumnDeserializer(Class<?> vc) {
        super(vc);
    }

    protected InputColumnDeserializer(JavaType valueType) {
        super(valueType);
    }

    protected InputColumnDeserializer(StdDeserializer<?> src) {
        super(src);
    }

    @Override
    public InputColumn deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
        InputColumn ret = new InputColumn();
        JsonNode node = p.getCodec().readTree(p);
        if (node.isNull()) {
            return null;
        }
        if (node.isTextual()) {
            // Legacy handling
            ret.setSourceTag("*");
            ret.setSourceColumnName(node.asText());
        } else {
            // New handling
            ret.setSourceTag(node.has("sourceTag") ? node.get("sourceTag").asText() : null);
            ret.setSourceTag(node.has("sourceColumnName") ? node.get("sourceColumnName").asText() : null);
        }

        return ret;
    }
}
