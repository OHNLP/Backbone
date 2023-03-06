package org.ohnlp.backbone.api.components;

import com.fasterxml.jackson.databind.JsonNode;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

public interface UsesLegacyConfigInit {
    void initFromConfig(JsonNode node) throws ComponentInitializationException;
}
