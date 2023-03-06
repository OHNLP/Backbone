package org.ohnlp.backbone.api.components.legacy.v2;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.schemas.Schema;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

public interface UsesLegacyConfigInit {
    void initFromConfig(JsonNode node) throws ComponentInitializationException;

    Schema calculateOutputSchema(Schema input);
}
