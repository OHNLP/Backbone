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
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.io.mongodb.MongoDBExtract;

import java.util.*;
import java.util.stream.Collectors;

public class BigQueryExtract extends Extract {
    @ConfigurationProperty(
            path = "query",
            desc = "The BigQuery query to use. Can be a tablespec (e.g. project_id:dataset_id.table_id) or a full query"
    )
    private String query; // SELECT * FROM project_id:dataset_id.table_id

    @ConfigurationProperty(
            path = "schema",
            desc = "The schema of the input data"
    )
    private Schema schema;

    @Override
    public void init() throws ComponentInitializationException {
    }

    @Override
    public Schema calculateOutputSchema(Schema input) {
        return this.schema;
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
