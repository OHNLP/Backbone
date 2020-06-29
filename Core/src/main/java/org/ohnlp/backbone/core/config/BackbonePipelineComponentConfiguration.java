package org.ohnlp.backbone.core.config;

import com.fasterxml.jackson.databind.JsonNode;
import org.ohnlp.backbone.api.BackbonePipelineComponent;

/**
 * Represents a declaration and associated configuration of a specific pipeline component.
 */
public class BackbonePipelineComponentConfiguration {
    /**
     * The class of the pipeline component, should extend {@link BackbonePipelineComponent}
     */
    private Class<? extends BackbonePipelineComponent> clazz;
    /**
     * The configuration associated with this specific component
     */
    private JsonNode config;

    public Class<? extends BackbonePipelineComponent> getClazz() {
        return clazz;
    }

    public void setClazz(Class<? extends BackbonePipelineComponent> clazz) {
        this.clazz = clazz;
    }

    public JsonNode getConfig() {
        return config;
    }

    public void setConfig(JsonNode config) {
        this.config = config;
    }
}
