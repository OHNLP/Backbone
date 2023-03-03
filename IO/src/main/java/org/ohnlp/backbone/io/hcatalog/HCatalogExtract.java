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
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
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
    @ConfigurationProperty(
            path = "metastore_uris",
            desc = "The HCatalog metastore URIs in the format thrift://metastore-host:port"
    )
    private String metaStoreURIs;
    @ConfigurationProperty(
            path = "database",
            desc = "The HCatalog database to read from"
    )
    private String database;
    @ConfigurationProperty(
            path = "table",
            desc = "The HCatalog table to read from"
    )
    private String table;

    @Override
    public void init() throws ComponentInitializationException {
        this.configProperties = new HashMap<>();
        this.configProperties.put("hive.metastore.uris", metaStoreURIs);
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
