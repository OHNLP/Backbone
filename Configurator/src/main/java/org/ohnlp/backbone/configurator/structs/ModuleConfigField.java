package org.ohnlp.backbone.configurator.structs;

import org.ohnlp.backbone.configurator.structs.types.TypedConfigurationField;

public class ModuleConfigField {
    private String path;
    private String desc;
    private boolean required;

    private TypedConfigurationField impl;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public TypedConfigurationField getImpl() {
        return impl;
    }

    public void setImpl(TypedConfigurationField impl) {
        this.impl = impl;
    }
}
