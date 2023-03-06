package org.ohnlp.backbone.io.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@ComponentDescription(
        name = "Extract Data from Big Query",
        desc = "Retrieves data from big query either via query or from an entire table specification with a 1:1 column name mapping"
)
public class BigQueryExtract extends Extract {
    @ConfigurationProperty(
            path = "query",
            desc = "The BigQuery query to use. Can be a tablespec (e.g. project_id:dataset_id.table_id) or a full query"
    )
    private String query; // SELECT * FROM project_id:dataset_id.table_id

    @ConfigurationProperty(
            path = "schema",
            desc = "The schema of the input data. Should match either the table spec or the query output if used"
    )
    private Schema schema;

    @Override
    public void init() throws ComponentInitializationException {
    }

    @Override
    public List<String> getOutputTags() {
        return Collections.singletonList("BigQuery Results");
    }

    @Override
    public Map<String, Schema> calculateOutputSchema(Map<String, Schema> input) {
        return Collections.singletonMap(getOutputTags().get(0), this.schema);
    }

    @Override
    public PCollectionRowTuple expand(PBegin input) {
        return PCollectionRowTuple.of(
                getOutputTags().get(0),
                input.apply("Read From BigQuery", BigQueryIO.readTableRows().fromQuery(this.query))
                .apply("Convert BigQuery TableRows to Beam Rows", ParDo.of(
                        new DoFn<TableRow, Row>() {
                            @ProcessElement
                            public void processElement(@Element TableRow input, OutputReceiver<Row> out) {
                                out.output(BigQueryUtils.toBeamRow(schema, input));
                            }
                        }
                )).setRowSchema(this.schema));
    }
}
