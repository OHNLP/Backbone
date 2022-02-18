package org.ohnlp.backbone.io.solr;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.solr.common.SolrDocument;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

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
public class SolrExtract extends Extract {
    private String solrHost;
    private String user;
    private String pass;
    private String query;
    private String docIdField;
    private String docTextField;
    private String collection;

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        this.solrHost = config.get("host").asText();
        this.user = config.get("user").asText();
        this.pass = config.get("password").asText();
        this.query = config.get("query").asText();
        this.docIdField = config.get("doc_id_field").asText();
        this.docTextField = config.get("doc_text_field").asText();
        this.collection = config.get("collection").asText();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
        Schema rowSchema = Schema.builder()
                .addStringField("note_id")
                .addStringField("note_text").build();
        SolrIO.ConnectionConfiguration config = SolrIO.ConnectionConfiguration.create(solrHost);
        if (!user.equals("NONE")) {
            config = config.withBasicCredentials(user, pass);
        }
        return SolrIO
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
                }));
    }
}
