package org.ohnlp.backbone.io.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.ohnlp.backbone.api.Load;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class BigQueryLoad extends Load {
    private String tablespec;
    private Schema schema;

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        this.tablespec = config.get("dest_table").asText();
        JsonNode schemaConfig = config.get("schema");
        List<Schema.Field> fields = new ArrayList<>();
        schemaConfig.fields().forEachRemaining((e) -> {
            fields.add(Schema.Field.of(e.getKey(), Schema.FieldType.of(Schema.TypeName.valueOf(e.getValue().asText()))));
        });
        this.schema = Schema.of(fields.toArray(new Schema.Field[0]));
    }

    @Override
    public POutput expand(PCollection<Row> input) {
        return input.apply(
                "Transform output rows to BigQuery TableRow format", ParDo.of(
                        new DoFn<Row, TableRow>() {
                            @ProcessElement
                            public void processElement(@Element Row row, OutputReceiver<TableRow> out) {
                                out.output(BigQueryUtils.toTableRow(row));
                            }
                        }
                )
        ).apply(
                "Write to BigQuery",
                BigQueryIO.writeTableRows()
                        .to(this.tablespec)
                        .withSchema(BigQueryUtils.toTableSchema(schema))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
        );

    }
}
