package org.ohnlp.backbone.api.components.xlang.python;

import java.util.List;
import java.util.Map;

public interface PythonHasOutputs {
    /**
     * @return A list of output tags
     */
    List<String> getOutputTags();

    /**
     * @param inputSchema The schema of the input being fed into this component
     * @return The expected output schema from this component
     */
    Map<String, String> calculateOutputSchemas(Map<String, String> inputSchema);
}
