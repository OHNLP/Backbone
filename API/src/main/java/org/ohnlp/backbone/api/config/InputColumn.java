package org.ohnlp.backbone.api.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.ohnlp.backbone.api.config.serialization.InputColumnDeserializer;

@JsonDeserialize(using = InputColumnDeserializer.class)
public class InputColumn {
    private String sourceTag;
    private String sourceColumnName;

    public String getSourceTag() {
        return sourceTag;
    }

    public void setSourceTag(String sourceTag) {
        this.sourceTag = sourceTag;
    }

    public String getSourceColumnName() {
        return sourceColumnName;
    }

    public void setSourceColumnName(String sourceColumnName) {
        this.sourceColumnName = sourceColumnName;
    }
}
