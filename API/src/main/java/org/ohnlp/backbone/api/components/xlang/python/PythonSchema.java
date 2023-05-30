package org.ohnlp.backbone.api.components.xlang.python;

import java.util.List;

public interface PythonSchema {
    interface PythonSchemaField {

    }

    List<PythonSchemaField> get_fields();

    List<PythonSchemaField> get_field(String name);
}
