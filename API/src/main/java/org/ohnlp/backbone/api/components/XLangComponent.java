package org.ohnlp.backbone.api.components;

import com.fasterxml.jackson.databind.JsonNode;
import org.ohnlp.backbone.api.BackbonePipelineComponent;

/**
 * Defines that this component is a cross-language component
 * <br/>
 * This interface is used as cross-language components do not support injection of configuration values, meaning that
 * external injection of the config JSON is necessary before {@link BackbonePipelineComponent#init()} is called
 */
public interface XLangComponent {
    void injectConfig(JsonNode config);
}
