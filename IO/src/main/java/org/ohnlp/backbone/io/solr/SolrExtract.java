package org.ohnlp.backbone.io.solr;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.solr.common.SolrDocument;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Reads in documents from Solr
 *
 * configuration format:
 * <code>
 *     {
 *         "host": "solrHost",
 *         "user": "solrUser or NONE if no credentials needed",
 *         "password": "solrPass or NONE if no credentials needed",
 *         "collection": "collection name",
 *         "query": "solr query to run",
 *         "doc_id_field": "field_name_for_document_id",
 *         "doc_text_field": "field_name_for_document_text"
 *     }
 * </code>
 */
// TODO currently input is limited strictly to three fields, make this more flexible
// Most likely possibility is to see a schema.xml as input
@ComponentDescription(
        name = "Read Records from Solr",
        desc = "Reads Records from Solr. Note that this is currently limited to strictly two fields (note_id/txt)"
)
public class SolrExtract extends Extract {
    @ConfigurationProperty(
            path = "host",
            desc = "The Solr Host"
    )
    private String solrHost;
    @ConfigurationProperty(
            path = "user",
            desc = "The Solr Username or NONE if no credentials needed",
            required = false
    )
    private String user = "NONE";
    @ConfigurationProperty(
            path = "password",
            desc = "The Solr password or NONE if no credentials needed",
            required = false
    )
    private String pass = "NONE";
    @ConfigurationProperty(
            path = "collection",
            desc = "The Solr collection to retrieve from"
    )
    private String collection;
    @ConfigurationProperty(
            path = "query",
            desc = "The Solr query to run"
    )
    private String query;
    @ConfigurationProperty(
            path = "doc_id_field",
            desc = "The field to use as document ids"
    )
    private String docIdField;
    @ConfigurationProperty(
            path = "doc_text_field",
            desc = "The field to use as document text"
    )
    private String docTextField;
    private Schema schema; // TODO switch to using this as input instead

    @Override
    public void init() throws ComponentInitializationException {
    }

    @Override
    public List<String> getOutputTags() {
        return Collections.singletonList("Solr Records: " + this.collection);
    }

    @Override
    public Map<String, Schema> calculateOutputSchema(Map<String, Schema> input) {
        this.schema = Schema.builder()
                .addStringField("note_id")
                .addStringField("note_text").build();
        return Collections.singletonMap(getOutputTags().get(0), this.schema);
    }


    @Override
    public PCollectionRowTuple expand(PBegin input) {
        Schema rowSchema = Schema.builder()
                .addStringField("note_id")
                .addStringField("note_text").build();
        SolrIO.ConnectionConfiguration config = SolrIO.ConnectionConfiguration.create(solrHost);
        if (!user.equals("NONE")) {
            config = config.withBasicCredentials(user, pass);
        }
        return PCollectionRowTuple.of(
                getOutputTags().get(0),
                SolrIO
                .read()
                .withConnectionConfiguration(config)
                .withQuery(this.query)
                .from(this.collection)
                .expand(input)
                .apply("Convert Solr Documents to Rows", ParDo.of(new DoFn<SolrDocument, Row>() {
                    @ProcessElement
                    public void processElement(@Element SolrDocument input, OutputReceiver<Row> output) {
                        output.output(Row.withSchema(rowSchema).addValues(
                                input.getFieldValue(docIdField),
                                input.getFieldValue(docTextField)).build());
                    }
                }))
                .setRowSchema(schema));
    }
}
