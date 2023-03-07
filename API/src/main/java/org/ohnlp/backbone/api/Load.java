package org.ohnlp.backbone.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.components.legacy.v2.UsesLegacyConfigInit;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.util.logging.Logger;

/**
 * Represents a component that performs the load (output) part of an ETL process
 *
 * It is assumed that data will be transformed into beam {@link Row}s prior to this step
 */
@Deprecated
public abstract class Load extends BackbonePipelineComponent<PCollection<Row>, POutput> implements UsesLegacyConfigInit {
    public Load() {
        Logger.getGlobal().warning(this.getClass().getSimpleName() + " is built against an old version of backbone. " +
                "While functionality has been retained via proxy classes for backwards compatibility purposes, these legacy classes should be " +
                "considered deprecated and may be retired in the future. Please check for updates for the module from " +
                "which this class is derived, and/or update the module to be built against the latest version of Backbone");
    }

    @Override
    public void init() {}
    public void initFromConfig(JsonNode node) throws ComponentInitializationException {};

    public Schema calculateOutputSchema(Schema input) {
        return input;
    }
}
