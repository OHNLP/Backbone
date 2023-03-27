package org.ohnlp.backbone.configurator.structs.types;

public class InputColumnTypedConfigurationField implements TypedConfigurationField {
    private final boolean isColumnList;

    public InputColumnTypedConfigurationField(boolean isColumnList) {
        this.isColumnList = isColumnList;
    }
}
