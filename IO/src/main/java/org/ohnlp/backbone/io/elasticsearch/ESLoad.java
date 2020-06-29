package org.ohnlp.backbone.io.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Load;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

/**
 * Performs data load to an Elasticsearch Data Sink
 */
public class ESLoad extends Load {

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {

    }

    @Override
    public PDone expand(PCollection<Row> input) {
        return null;
    }
}
