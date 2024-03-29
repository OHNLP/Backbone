package org.ohnlp.backbone.io.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.components.LoadFromOne;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

@ComponentDescription(
        name = "Write Records into BigQuery",
        desc = "Appends the input collection into a BigQuery table, or creates the table if it does not exist"
)
public class BigQueryLoad extends LoadFromOne {

    @ConfigurationProperty(
            path = "dest_table",
            desc = "The destination BigQuery table specification (e.g. project_id:dataset_id.table_id) to connect to."
    )
    private String tablespec;
    private Schema writeSchema;

    @Override
    public void init() throws ComponentInitializationException {
    }

    @Override
    public POutput expand(PCollection<Row> input) {
        this.writeSchema = input.getSchema();
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
                        .withSchema(BigQueryUtils.toTableSchema(this.writeSchema))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
        );
    }
}
