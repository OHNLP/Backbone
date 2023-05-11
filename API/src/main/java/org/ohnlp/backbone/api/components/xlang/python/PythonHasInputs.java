package org.ohnlp.backbone.api.components.xlang.python;

import java.util.List;

public interface PythonHasInputs {
    /**
     * @return A list of accepted input tags
     */
    List<String> getInputTags();

    /**
     * @param inputTag the inputTag to get required columns for
     * @return A schema json representation of the required columns present in the input for the specified tag
     */
    String getRequiredColumns(String inputTag);
}
