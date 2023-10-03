package org.ohnlp.backbone.io.local.encodings;

import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.List;
import java.util.stream.Collectors;

public class RowHeaderToPipeDelimitedEncoding extends RowToTextEncoding {
    @Override
    public String toTextWithFields(Row input, List<String> fields) {
        return String.join("|", fields);
    }

    @Override
    public String toTextAllFields(Row input) {
        return input.getSchema().getFieldNames().stream().map(StringEscapeUtils::escapeCsv).collect(Collectors.joining("|"));
    }
}
