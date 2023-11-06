package org.ohnlp.backbone.io.local.encodings;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class RowValueToCSVEncoding extends RowToTextEncoding {
    @Override
    public String toTextWithFields(Row input, List<String> fields) {
        return fields.stream().map(f -> parseFieldToString(input.getValue(f), input.getSchema().getField(f))).collect(Collectors.joining(","));
    }

    @Override
    public String toTextAllFields(Row input) {
        return input.getSchema().getFields().stream().map(f -> parseFieldToString(input.getValue(f.getName()), f)).collect(Collectors.joining(","));
    }

    private String parseFieldToString(Object o, Schema.Field f) {
        if (o == null) {
            return "";
        } else {
            Schema.FieldType type = f.getType();
            if (type.getTypeName().isPrimitiveType()) {
                return StringEscapeUtils.escapeCsv(o.toString());
            } else {
                try {
                    if (o instanceof Serializable) {
                        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                            oos.writeObject(o);
                            oos.flush();
                            return StringEscapeUtils.escapeCsv(Hex.encodeHexString(bos.toByteArray()));
                        }
                    } else {
                        throw new IllegalArgumentException("Trying to Serialize non-Serializable Type " + o.getClass().getName());
                    }
                } catch (Throwable e) {
                    throw new RuntimeException("Failed to Serialize as JSON: " + o, e);
                }
            }

        }
    }
}
