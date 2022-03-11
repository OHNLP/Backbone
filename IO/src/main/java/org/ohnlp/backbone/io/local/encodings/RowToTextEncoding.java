package org.ohnlp.backbone.io.local.encodings;

import org.apache.beam.sdk.values.Row;

import java.io.Serializable;
import java.util.List;

public abstract class RowToTextEncoding implements Serializable {

    public String toText(Row input, List<String> fields) {
        if (fields == null || fields.size() == 0) {
            return toTextAllFields(input);
        } else {
            return toTextWithFields(input, fields);
        }
    }

    public abstract String toTextWithFields(Row input, List<String> fields);
    public abstract String toTextAllFields(Row input);
}
