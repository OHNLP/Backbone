package org.ohnlp.backbone.io.local.encodings;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.apache.beam.sdk.values.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowToJSONEncoding extends RowToTextEncoding {

    private transient ObjectWriter ow;

    @Override
    public String toTextWithFields(Row input, List<String> fields) {
        if (ow == null) {
            ow = new ObjectMapper().registerModule(new JodaModule()).writer();
        }
        Map<String, Object> out = new HashMap<>();
        fields.forEach(f -> out.put(f, input.getValue(f)));
        try {
            return ow.writeValueAsString(out);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toTextAllFields(Row input) {
        if (ow == null) {
            ow = new ObjectMapper().registerModule(new JodaModule()).writer();
        }
        Map<String, Object> out = new HashMap<>();
        input.getSchema().getFieldNames().forEach(f -> out.put(f, input.getValue(f)));
        try {
            return ow.writeValueAsString(out);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
