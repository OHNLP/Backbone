package org.ohnlp.backbone.configurator.structs.types;

public class MapTypedConfigurationField implements TypedConfigurationField {
    private final TypedConfigurationField key;
    private final TypedConfigurationField value;

    public MapTypedConfigurationField(TypedConfigurationField key, TypedConfigurationField value) {
        this.key = key;
        this.value = value;
    }
}
