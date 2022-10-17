package org.ohnlp.backbone.io.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.io.mongodb.MongoDBExtract;

import java.util.*;
import java.util.stream.Collectors;

public class BigQueryExtract extends Extract {
    private String query; // SELECT * FROM project_id:dataset_id.table_id
    private Schema schema;

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        this.query = config.get("query").asText();
        initSchema(config.get("schema"));
    }

    @Override
    public Schema calculateOutputSchema(Schema input) {
        return this.schema;
    }

    private void initSchema(JsonNode schema) {
        Schema.Builder schemaBuilder = Schema.builder();

        schema.fields().forEachRemaining(e -> {
            String fieldName = e.getKey();
            Schema.FieldType type;
            try {
                type = (Schema.FieldType) Schema.FieldType.class.getDeclaredField(e.getValue().asText().toUpperCase(Locale.ROOT)).get(null);
            } catch (IllegalAccessException | NoSuchFieldException ex) {
                throw new RuntimeException(ex);
            }
            schemaBuilder.addField(fieldName, type);
        });
        this.schema = schemaBuilder.build();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
        return input.apply("Read From BigQuery", BigQueryIO.readTableRows().fromQuery(this.query))
                .apply("Convert BigQuery TableRows to Beam Rows", ParDo.of(
                        new DoFn<TableRow, Row>() {
                            @ProcessElement
                            public void processElement(@Element TableRow input, OutputReceiver<Row> out) {
                                out.output(BigQueryUtils.toBeamRow(schema, input));
                            }
                        }
                ));
    }
}
