package org.ohnlp.backbone.configurator.structs.types;

public class CollectionTypedConfigurationField implements TypedConfigurationField {
    TypedConfigurationField contents;

    public TypedConfigurationField getContents() {
        return contents;
    }

    public void setContents(TypedConfigurationField contents) {
        this.contents = contents;
    }
}
