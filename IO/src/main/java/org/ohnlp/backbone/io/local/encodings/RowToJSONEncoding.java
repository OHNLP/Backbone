package org.ohnlp.backbone.io.local.encodings;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.beam.sdk.values.Row;

import java.util.HashMap;
import java.util.Map;

public class RowToJSONEncoding extends RowToTextEncoding {
    private transient ThreadLocal<ObjectWriter> ow = ThreadLocal.withInitial(() -> new ObjectMapper().writer());
    @Override
    public String toText(Row input) {
        Map<String, Object> out = new HashMap<>();
        input.getSchema().getFieldNames().forEach(f -> out.put(f, input.getValue(f)));
        try {
            return ow.get().writeValueAsString(out);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
