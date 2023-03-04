package org.ohnlp.backbone.core.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import org.ohnlp.backbone.api.BackbonePipelineComponent;

import java.util.List;

/**
 * Represents a declaration and associated configuration of a specific pipeline component.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BackbonePipelineComponentConfiguration {

    /**
     * An ID for this step. Defaults to the numeric index of this step in the pipeline
     */
    private String stepId;

    /**
     * IDs for input step. Defaults to a single input at (numeric index - 1)
     */
    private List<String> inputIds;

    /**
     * The class of the pipeline component, should extend {@link BackbonePipelineComponent}
     */
    private Class<? extends BackbonePipelineComponent> clazz;
    /**
     * The configuration associated with this specific component
     */
    private JsonNode config;

    public String getStepId() {
        return stepId;
    }

    public void setStepId(String stepId) {
        this.stepId = stepId;
    }

    public List<String> getInputIds() {
        return inputIds;
    }

    public void setInputIds(List<String> inputIds) {
        this.inputIds = inputIds;
    }

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
