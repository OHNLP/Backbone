package org.ohnlp.backbone.api.exceptions;

/**
 * Indicates that a component failed to initialize correctly (e.g. due to bad configuration)
 */
public class ComponentInitializationException extends Exception {
    public ComponentInitializationException(Throwable t) {
        super(t);
    }
}
