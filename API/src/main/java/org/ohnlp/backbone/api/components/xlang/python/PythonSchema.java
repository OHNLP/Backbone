package org.ohnlp.backbone.api.components.xlang.python;

import java.util.List;

public interface PythonSchema {
    interface PythonSchemaField {
        String get_name();
        PythonFieldType get_field_type();
    }
    interface PythonFieldType {
        String get_type_name();
        PythonFieldType get_array_content_type();
        PythonSchema get_content_obj_fields();
    }

    List<PythonSchemaField> get_fields();

    PythonSchemaField get_field(String name);
}
