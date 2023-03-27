package org.ohnlp.backbone.configurator.structs.types;

import java.util.Map;

public class ObjectTypedConfigurationField implements TypedConfigurationField {
    private final Map<String, TypedConfigurationField> fields;

    public ObjectTypedConfigurationField(Map<String, TypedConfigurationField> fields) {
        this.fields = fields;
    }
}
