package org.ohnlp.backbone.io.local.encodings;

import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.stream.Collectors;

public class RowValueToCSVEncoding extends RowToTextEncoding {
    @Override
    public String toText(Row input) {
        return input.getValues().stream().map(o -> StringEscapeUtils.escapeCsv(o.toString())).collect(Collectors.joining(","));
    }
}
