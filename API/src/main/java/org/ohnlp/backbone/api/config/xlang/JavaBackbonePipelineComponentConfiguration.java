package org.ohnlp.backbone.api.config.xlang;

import org.ohnlp.backbone.api.BackbonePipelineComponent;
import org.ohnlp.backbone.api.ComponentLang;
import org.ohnlp.backbone.api.config.BackbonePipelineComponentConfiguration;

public class JavaBackbonePipelineComponentConfiguration extends BackbonePipelineComponentConfiguration {

    public JavaBackbonePipelineComponentConfiguration() {
        setLang(ComponentLang.JAVA);
    }

    /**
     * The class of the pipeline component, should extend {@link BackbonePipelineComponent}
     */
    private Class<? extends BackbonePipelineComponent> clazz;

    public Class<? extends BackbonePipelineComponent> getClazz() {
        return clazz;
    }

    public void setClazz(Class<? extends BackbonePipelineComponent> clazz) {
        this.clazz = clazz;
    }
}
