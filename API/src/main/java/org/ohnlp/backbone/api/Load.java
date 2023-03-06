package org.ohnlp.backbone.api;

import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.components.LoadFromOne;

import java.util.logging.Logger;

/**
 * Represents a component that performs the load (output) part of an ETL process
 *
 * It is assumed that data will be transformed into beam {@link Row}s prior to this step
 */
@Deprecated
public abstract class Load extends LoadFromOne {
    public Load() {
        Logger.getGlobal().warning(this.getClass().getSimpleName() + " is built against an old version of backbone." +
                "While functionality has been retained via proxy classes for backwards compatibility purposes, these legacy classes should be " +
                "considered deprecated and may be retired in the future. Please check for updates for the module from " +
                "which this class is derived, and/or update the module to be built against the latest version of Backbone");
    }
}
