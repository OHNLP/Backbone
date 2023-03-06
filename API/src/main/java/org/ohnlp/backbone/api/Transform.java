package org.ohnlp.backbone.api;

import org.ohnlp.backbone.api.components.OneToOneTransform;

import java.util.logging.Logger;

@Deprecated
public abstract class Transform extends OneToOneTransform {

    public Transform() {
        Logger.getGlobal().warning(this.getClass().getSimpleName() + " is built against an old version of backbone." +
                "While functionality has been retained via proxy classes for backwards compatibility purposes, these legacy classes should be " +
                "considered deprecated and may be retired in the future. Please check for updates for the module from " +
                "which this class is derived, and/or update the module to be built against the latest version of Backbone");
    }
}
