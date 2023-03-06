package org.ohnlp.backbone.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.components.legacy.v2.UsesLegacyConfigInit;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.util.logging.Logger;

@Deprecated
public abstract class Transform extends BackbonePipelineComponent<PCollection<Row>, PCollection<Row>> implements UsesLegacyConfigInit {

    public Transform() {
        Logger.getGlobal().warning(this.getClass().getSimpleName() + " is built against an old version of backbone." +
                "While functionality has been retained via proxy classes for backwards compatibility purposes, these legacy classes should be " +
                "considered deprecated and may be retired in the future. Please check for updates for the module from " +
                "which this class is derived, and/or update the module to be built against the latest version of Backbone");
    }

    @Override
    public void init() {}
    public void initFromConfig(JsonNode node) throws ComponentInitializationException {};
}
