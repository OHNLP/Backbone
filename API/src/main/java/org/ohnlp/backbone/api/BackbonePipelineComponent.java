package org.ohnlp.backbone.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

/**
 * Represents a configurable pipeline component used in the OHNLP backbone
 */
public interface BackbonePipelineComponent {
    /**
     * Initializes the component from a specified JSON configuration. Note that any encrypted properties will have been
     * decrypted to plaintext form at this stage via Jasypt
     *
     * @param config The configuration section pertaining to this component
     * @throws ComponentInitializationException if an error occurs during initialization or if configuraiton contains
     *                                          unexpected values
     */
    void initFromConfig(JsonNode config) throws ComponentInitializationException;
}
