package org.ohnlp.backbone.api.config;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.JsonNode;
import org.ohnlp.backbone.api.BackbonePipelineComponent;
import org.ohnlp.backbone.api.ComponentLang;
import org.ohnlp.backbone.api.config.xlang.JavaBackbonePipelineComponentConfiguration;
import org.ohnlp.backbone.api.config.xlang.PythonBackbonePipelineComponentConfiguration;

import java.util.Map;
import java.util.Objects;

/**
 * Represents a declaration and associated configuration of a specific pipeline component.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "lang",
        visible = true,
        defaultImpl = JavaBackbonePipelineComponentConfiguration.class
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = JavaBackbonePipelineComponentConfiguration.class, name="JAVA"),
        @JsonSubTypes.Type(value = PythonBackbonePipelineComponentConfiguration.class, name="PYTHON")
})
public class BackbonePipelineComponentConfiguration {

    /**
     * The component lang
     */
    private ComponentLang lang;

    /**
     * An ID for this step. Defaults to the numeric index of this step in the pipeline
     */
    private String componentID;

    /**
     * Input mappings
     */
    private Map<String, InputDefinition> inputs;

    /**
     * The configuration associated with this specific component
     */
    private JsonNode config;

    public ComponentLang getLang() {
        return lang;
    }

    public void setLang(ComponentLang lang) {
        this.lang = lang;
    }

    public String getComponentID() {
        return componentID;
    }

    public void setComponentID(String componentID) {
        this.componentID = componentID;
    }

    public Map<String, InputDefinition> getInputs() {
        return inputs;
    }

    public void setInputs(Map<String, InputDefinition> inputs) {
        this.inputs = inputs;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InputDefinition that = (InputDefinition) o;
            return Objects.equals(componentID, that.componentID) && Objects.equals(inputTag, that.inputTag);
        }

        @Override
        public int hashCode() {
            return Objects.hash(componentID, inputTag);
        }
    }
}
