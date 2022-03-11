package org.ohnlp.backbone.io.local.encodings;

import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.stream.Collectors;

public class RowHeaderToCSVEncoding extends RowToTextEncoding {
    @Override
    public String toText(Row input) {
        return input.getSchema().getFieldNames().stream().map(StringEscapeUtils::escapeCsv).collect(Collectors.joining(","));
    }
}
