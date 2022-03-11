package org.ohnlp.backbone.io.local.encodings;

import org.apache.beam.sdk.values.Row;

import java.io.Serializable;

public abstract class RowToTextEncoding implements Serializable {
    public abstract String toText(Row input);
}
