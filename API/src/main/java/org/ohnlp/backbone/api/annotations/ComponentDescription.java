package org.ohnlp.backbone.api.annotations;

public @interface ComponentDescription {
    String name();
    String desc();
    String[] requires() default {};
}
