package org.ohnlp.backbone.io.mongodb;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.bson.Document;
import org.ohnlp.backbone.api.Load;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.io.local.encodings.RowToJSONEncoding;

import java.util.ArrayList;

/**
 * Writes to MongoDB
 * <p>
 * Expected configuration:
 * <pre>
 *     {
 *         "uri": "mongodb://[username:password@]host[:port1][,...hostN[:portN]]",
 *         "database": "your database name",
 *         "collection": "your collection name",
 *         "batch_size": 1000 (recommended default, # of records to write per batch)
 *         "fields": ["An", "Optional", "Listing", "of", "Row", "Fields", "to", "Subset", "for", "Output"]
 *     }
 * </pre>
 * <p>
 * For full listing of possible URI formats, please consult <a href="https://www.mongodb.com/docs/manual/reference/connection-string/">MongoDB Documentation</a>
 */
public class MongoDBLoad extends Load {
    private String uri;
    private String database;
    private String collection;
    private ArrayList<String> fields;
    private int batchSize;
    private MongoDbIO.Write mongoInterface;
    private RowToJSONEncoding jsonEncoder;

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        this.uri = config.get("uri").asText();
        this.database = config.get("database").asText();
        this.collection = config.get("collection").asText();
        this.batchSize = config.has("batch_size") ? config.get("batch_size").asInt() : 1000;
        this.fields = new ArrayList<>();
        if (config.has("fields")) {
            for (JsonNode field : config.get("fields")) {
                fields.add(field.asText());
            }
        }
        this.mongoInterface = MongoDbIO.write()
                .withUri(uri)
                .withDatabase(database)
                .withCollection(collection)
                .withBatchSize(batchSize);
        this.jsonEncoder = new RowToJSONEncoding();
    }

    @Override
    public PDone expand(PCollection<Row> input) {
        return mongoInterface.expand(
                input.apply("Map to MongoDB Document Types", ParDo.of(new DoFn<Row, Document>() {
                    @ProcessElement
                    public void processElement(@Element Row row, OutputReceiver<Document> out) {
                        out.output(Document.parse(jsonEncoder.toText(row, fields)));
                    }
                }))
        );
    }
}
