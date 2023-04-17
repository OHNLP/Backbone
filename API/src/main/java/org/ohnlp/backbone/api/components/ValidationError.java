package org.ohnlp.backbone.api.components;

/**
 * Describes an error that occurs when validating pipeline components and associated configurations
 */
public class ValidationError extends Exception {
    private final ValidationErrorPriority priority;

    public enum ValidationErrorPriority {
        CRITICAL,
        WARNING,
        INFO
    }

    public ValidationError(ValidationErrorPriority priority, String message) {
        super(message);
        this.priority = priority;
    }

    public ValidationError(ValidationErrorPriority priority, String message, Throwable cause) {
        super(message, cause);
        this.priority = priority;
    }

    public ValidationErrorPriority getPriority() {
        return priority;
    }
}
