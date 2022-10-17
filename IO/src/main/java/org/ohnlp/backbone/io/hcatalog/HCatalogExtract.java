package org.ohnlp.backbone.io.hcatalog;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.io.hcatalog.HCatToRow;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.util.HashMap;
import java.util.Map;

/**
 * Extracts items from HCatalog (e.g. Hive) stores. <br/>
 * Expected Configuration:
 * <code>
 *     {
 *         "metastore_uris": "thrift://metastore-host:port",
 *         "database": "source_database_name",
 *         "table": "source_table_name"
 *     }
 * </code>
 */
public class HCatalogExtract extends Extract {
    Map<String, String> configProperties;
    private String database;
    private String table;

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        this.configProperties = new HashMap<>();
        this.configProperties.put("hive.metastore.uris", config.get("metastore_uris").asText());
        this.database = config.get("database").asText();
        this.table = config.get("table").asText();
    }

    @Override
    public Schema calculateOutputSchema(Schema input) {
        return null;
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
        return HCatToRow.fromSpec(HCatalogIO.read().withConfigProperties(configProperties)
                .withDatabase(database)
                .withTable(table)).expand(input);
    }
}
