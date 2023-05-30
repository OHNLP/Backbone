package org.ohnlp.backbone.api.components.xlang.python;

import org.apache.beam.sdk.schemas.Schema;

public interface PythonStructureProxy {
    PythonSchema python_schema_from_json_string(String schema);
    PythonRow python_row_from_json_string(String row);
    String json_string_from_python_schema(PythonSchema schema);
    String json_string_from_python_row(PythonRow row);
}
