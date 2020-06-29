package org.ohnlp.backbone.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

/**
 * Represents a configurable pipeline component used in the OHNLP backbone
 */
public abstract class BackbonePipelineComponent<I extends PInput, O extends POutput> extends PTransform<I, O> {
    /**
     * Initializes the component from a specified JSON configuration. Note that any encrypted properties will have been
     * decrypted to plaintext form at this stage via Jasypt
     *
     * @param config The configuration section pertaining to this component
     * @throws ComponentInitializationException if an error occurs during initialization or if configuraiton contains
     *                                          unexpected values
     */
    public abstract void initFromConfig(JsonNode config) throws ComponentInitializationException;

    /**
     * @return A java class that represents {@link I}
     */
    public abstract Class<?> getInputType();
    /**
     * @return A java class that represents {@link O}
     */
    public abstract Class<?> getOutputType();
}
