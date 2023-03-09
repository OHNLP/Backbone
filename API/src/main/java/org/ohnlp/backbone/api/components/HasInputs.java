package org.ohnlp.backbone.api.components;

import org.apache.beam.sdk.schemas.Schema;

import java.util.List;

public interface HasInputs {
    List<String> getInputTags();
    boolean hasRequiredColumns();
    Schema getRequiredColumns();
}
