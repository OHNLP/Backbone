package org.ohnlp.backbone.configurator;

import org.ohnlp.backbone.configurator.structs.types.TypedConfigurationField;

public class NumericTypedConfigurationField implements TypedConfigurationField {
    private final boolean floating;
    private final Number minValue;
    private final Number maxValue;

    public NumericTypedConfigurationField(boolean floating, Number minValue, Number maxValue) {
        this.floating = floating;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }
}
