package org.ohnlp.backbone.io.local.encodings;

import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class RowValueToPipeDelimitedEncoding extends RowToTextEncoding {
    @Override
    public String toTextWithFields(Row input, List<String> fields) {
        return fields.stream().map(f -> StringEscapeUtils.escapeCsv(Optional.ofNullable(input.getValue(f)).orElse("").toString())).collect(Collectors.joining("|"));
    }

    @Override
    public String toTextAllFields(Row input) {
        return input.getValues().stream().map(o -> o == null ? "" : o.toString()).collect(Collectors.joining("|"));

    }
}
