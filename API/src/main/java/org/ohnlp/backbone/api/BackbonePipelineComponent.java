package org.ohnlp.backbone.api;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.ohnlp.backbone.api.components.ValidationError;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

/**
 * Represents a configurable pipeline component used in the OHNLP backbone
 */
public abstract class BackbonePipelineComponent<I extends PInput, O extends POutput> extends PTransform<I, O> {
    private Schema schema;
    private boolean schemaInit = false;

    /**
     * Initializes the component from a specified JSON configuration. Note that any encrypted properties will have been
     * decrypted to plaintext form at this stage via Jasypt.
     * <p>
     * Configuration properties as denoted by the {@link org.ohnlp.backbone.api.annotations.ConfigurationProperty}
     * annotation will be injected and populated prior to this step
     *
     * @throws ComponentInitializationException if an error occurs during initialization or if configuraiton contains
     *                                          unexpected values
     */
    public abstract void init() throws ComponentInitializationException;

    /**
     * Validates component configuration as supplied if implemented.
     * @throws ValidationError if a validation error occurs.
     */
    public abstract void validate() throws ValidationError;
}
