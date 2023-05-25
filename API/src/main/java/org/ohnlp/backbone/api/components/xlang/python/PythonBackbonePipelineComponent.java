package org.ohnlp.backbone.api.components.xlang.python;

import java.util.List;
import java.util.Map;

public interface PythonBackbonePipelineComponent {
    /**
     * Initialize the component with config json
     */
    void init(String json);

    /**
     * @return A String-based JSON config to pass to the DoFn on init
     */
    String toDoFnConfig();

    /**
     * @return A Tag describing the expected input
     */
    String getInputTag();

    /**
     * @return A list of one or more labels/tags for expected outputs
     */
    List<String> getOutputTags();

    /**
     * @param jsonifiedInputSchemas The schemas being input
     * @return A derived output schema from the supplied input schema
     */
    Map<String, String> calculateOutputSchema(Map<String, String> jsonifiedInputSchemas);
}
