package org.ohnlp.backbone.configurator.structs.types;

import java.util.List;

public class EnumerationTypedConfigurationField implements TypedConfigurationField {
    private final List<String> constants;

    public EnumerationTypedConfigurationField(List<String> constants) {
        this.constants = constants;
    }
}
