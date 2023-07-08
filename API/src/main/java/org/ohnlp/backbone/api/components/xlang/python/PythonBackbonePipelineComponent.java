package org.ohnlp.backbone.api.components.xlang.python;

import java.util.List;
import java.util.Map;

public interface PythonBackbonePipelineComponent extends PythonStructureProxy {
    /**
     * Initialize the component with config json
     */
    void init(String json);

    /**
     * @return A String-based JSON config to pass to the DoFn on init
     */
    String to_do_fn_config();

    /**
     * @return A Tag describing the expected input
     */
    String get_input_tag();

    /**
     * @return A list of one or more labels/tags for expected outputs
     */
    List<String> proxied_get_output_tags();

    /**
     * @param input_schemas The schemas being input
     * @return A derived output schema from the supplied input schema
     */
    Map<String, PythonSchema> proxied_calculate_output_schema(Map<String, PythonSchema> input_schemas);
}
