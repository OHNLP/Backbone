package org.ohnlp.backbone.io.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.services.bigquery.model.TableRow;
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
    private List<String> selectedFields;

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        this.selectedFields = new ArrayList<>();
        if (config.has("select")) {
            for (JsonNode fieldName : config.get("select")) {
                selectedFields.add(fieldName.asText());
            }
        }
        this.tablespec = config.get("dest_table").asText();
    }

    @Override
    public POutput expand(PCollection<Row> input) {
        if (this.selectedFields.size() > 0) {
            input = input.apply("Subset Columns", ParDo.of(new DoFn<Row, Row>() {
                @ProcessElement
                public void process(ProcessContext c) {
                    Row input = c.element();
                    // We have to dynamically resolve schema row by row because
                    // we don't store schema in the collection itself as it is user-configurable/dynamic
                    Schema inputSchema = input.getSchema();
                    List<Schema.Field> outputSchemaFields = new ArrayList<>();
                    for (String fieldName : selectedFields) {
                        outputSchemaFields.add(inputSchema.getField(fieldName));
                    }
                    Schema outSchema = Schema.of(outputSchemaFields.toArray(new Schema.Field[0]));
                    // And now just map the values
                    List<Object> outputValues = new ArrayList<>();
                    for (String s : selectedFields) {
                        outputValues.add(input.getValue(s));
                    }
                    c.output(Row.withSchema(outSchema).addValues(outputValues).build());
                }
            }));
        }
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
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
        );

    }
}
