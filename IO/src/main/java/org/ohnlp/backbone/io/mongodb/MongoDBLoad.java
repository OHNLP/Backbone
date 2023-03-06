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
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
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
@ComponentDescription(
        name = "Writes Records into MongoDB",
        desc = "Writes Records into MongoDB. An optional list of fields to subset can also be provided."
)
public class MongoDBLoad extends Load {
    @ConfigurationProperty(
            path = "uri",
            desc = "The MongoDB URI to connect to in the format mongodb://[username:password@]host[:port1][,...hostN[:portN]]"
    )
    private String uri;
    @ConfigurationProperty(
            path = "database",
            desc = "The MongoDB Database to connect to"
    )
    private String database;
    @ConfigurationProperty(
            path = "collection",
            desc = "The MongoDB Collection to connect to"
    )
    private String collection;
    @ConfigurationProperty(
            path = "batch_size",
            desc = "The number of documents to write per batch/partition. Decrease this if having memory issues",
            required = false
    )
    private int batchSize = 1000;
    @ConfigurationProperty(
            path = "fields",
            desc = "The specific document fields to retrieve. Leave blank for all fields",
            required = false
    )
    private ArrayList<String> fields = new ArrayList<>();
    private MongoDbIO.Write mongoInterface;
    private RowToJSONEncoding jsonEncoder;

    @Override
    public void init() throws ComponentInitializationException {
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
