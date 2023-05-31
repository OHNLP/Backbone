package org.ohnlp.backbone.api.components.xlang.python;

import java.util.List;

public interface PythonRow {
    PythonSchema get_schema();
    Object get_value();
    List<Object> get_values();

}
