package org.ohnlp.backbone.api.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import org.ohnlp.backbone.api.BackbonePipelineComponent;

import java.util.Map;

/**
 * Represents a declaration and associated configuration of a specific pipeline component.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BackbonePipelineComponentConfiguration {

    /**
     * An ID for this step. Defaults to the numeric index of this step in the pipeline
     */
    private String componentID;

    /**
     * Input mappings
     */
    private  Map<String, InputDefinition>  inputs;

    /**
     * The class of the pipeline component, should extend {@link BackbonePipelineComponent}
     */
    private Class<? extends BackbonePipelineComponent> clazz;
    /**
     * The configuration associated with this specific component
     */
    private JsonNode config;

    public String getComponentID() {
        return componentID;
    }

    public void setComponentID(String componentID) {
        this.componentID = componentID;
    }

    public Map<String, InputDefinition> getInputs() {
        return inputs;
    }

    public void setInputs( Map<String, InputDefinition>  inputs) {
        this.inputs = inputs;
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


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class InputDefinition {
        String componentID;
        String inputTag;

        public String getComponentID() {
            return componentID;
        }

        public void setComponentID(String componentID) {
            this.componentID = componentID;
        }

        public String getInputTag() {
            return inputTag;
        }

        public void setInputTag(String inputTag) {
            this.inputTag = inputTag;
        }
    }
}
