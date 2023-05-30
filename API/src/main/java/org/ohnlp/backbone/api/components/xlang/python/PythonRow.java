package org.ohnlp.backbone.api.components.xlang.python;

import java.util.List;

public interface PythonRow {
    PythonSchema get_row_schema();
    PythonRow append_value(Object value);
    PythonRow append_values(List<Object> values);
}
