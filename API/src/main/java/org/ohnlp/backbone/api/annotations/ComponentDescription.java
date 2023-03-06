package org.ohnlp.backbone.api.annotations;

import org.ohnlp.backbone.api.BackbonePipelineComponent;

import java.lang.reflect.Array;

public @interface ComponentDescription {
    String name();
    String desc();
    Class<? extends BackbonePipelineComponent<?,?>>[] requires() default {};
}
