package org.ohnlp.backbone.api.components.xlang.python;

import java.util.Map;

public interface PythonBackbonePipelineComponent {
    /**
     * Initialize the component with config json
     */
    void init(String json);

    /**
     * @return True if the schema has already been initialized
     */
    boolean isSchemaInit();

    /**
     * Sets the schema initialization state to true
     */
    void setSchemaInit();

    /**
     * Sets the output schema for this component
     * @param schemasByTag The output schema by tag
     */
    void setComponentSchema(Map<String, String> schemasByTag);

    /**
     * Get component output schema
     */
    Map<String, String> getComponentSchema();

    /**
     * @return The actual DoFn that determines what to do
     */
    PythonProcessingPartitionBasedDoFn<?, ?> getDoFn();
}
