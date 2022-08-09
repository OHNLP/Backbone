package org.ohnlp.backbone.io.mongodb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.beam.sdk.io.mongodb.AggregationQuery;
import org.apache.beam.sdk.io.mongodb.FindQuery;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.Document;
import org.joda.time.DateTime;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Reads from MongoDB
 * <p>
 * Expected configuration:
 * <pre>
 *     {
 *         "uri": "mongodb://[username:password@]host[:port1][,...hostN[:portN]]",
 *         "database": "your database name",
 *         "collection": "your collection name",
 *         "schema": {
 *             "field1": "TYPE",
 *             "field2": "TYPE",
 *             "complex_field1": {
 *                 "field3": "TYPE",
 *                 "field4": "TYPE"
 *             },
 *             "aggregate_pipeline": ["{}", "{}"] // Your MongoDB query function pipeline as a list of JSON-escaped strings
 *         }
 *     }
 * </pre>
 * <p>
 * For full listing of possible URI formats, please consult <a href="https://www.mongodb.com/docs/manual/reference/connection-string/">MongoDB Documentation</a>
 * <br/>
 * Schema corresponds to the schema of the input document <br/>
 * Accepted TYPEs are BOOLEAN, DATE, DOUBLE, INTEGER, LONG, STRING, and OBJECT_ID
 * <br/>
 * Note that embedded object will be flattened in the output row structure, e.g. values located under field3 can be
 * referred to using "complex_field1.field3" further on down the pipeline
 */
public class MongoDBExtract extends Extract {
    private String uri;
    private String database;
    private String collection;
    private Schema schema;
    private ArrayList<SchemaDefinition> mappings;
    private List<String> filter;

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        this.uri = config.get("uri").asText();
        this.database = config.get("database").asText();
        this.collection = config.get("collection").asText();
        initSchema(config.get("schema"));
        this.filter = new ArrayList<>();
        if (config.has("aggregate_pipeline")) {
            config.get("aggregate_pipeline").forEach(str -> {
                this.filter.add(str.asText());
            });
        }
    }

    private void initSchema(JsonNode schema) {
        LinkedList<String> pathQue = new LinkedList<>();
        Map<List<String>, PrimitiveType> mappings = new HashMap<>(); // TODO implement more complex types (e.g. embedded objects and arrays)
        parseSchema(schema, pathQue, mappings);
        this.mappings = mappings.entrySet().stream().map(e -> new SchemaDefinition(e.getKey(), e.getValue())).collect(Collectors.toCollection(ArrayList::new)); // Use list to ensure consistant iteration order between mapping/schema
        Schema.Builder schemaBuilder = Schema.builder();
        this.mappings.forEach(e -> {
            schemaBuilder.addField(Schema.Field.of(String.join(".", e.getKey()), e.getType().beamType));
        });
        this.schema = schemaBuilder.build();
    }

    private void parseSchema(JsonNode currNode, LinkedList<String> pathQue, Map<List<String>, PrimitiveType> mappings) {
        currNode.fieldNames().forEachRemaining(field -> {
            pathQue.addLast(field);
            if (currNode.get(field) instanceof ObjectNode) {
                parseSchema(currNode.get(field), pathQue, mappings);
            } else {
                // else if instanceof ArrayNode
                PrimitiveType type = PrimitiveType.valueOf(currNode.get(field).asText().toUpperCase(Locale.ROOT));
                mappings.put(new LinkedList<>(pathQue), type);
            }
            pathQue.removeLast();
        });
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
        MongoDbIO.Read readFn = MongoDbIO.read()
                .withUri(this.uri)
                .withDatabase(this.database)
                .withCollection(this.collection);
        if (this.filter.size() > 0) {
            readFn = readFn.withQueryFn(AggregationQuery
                    .create()
                    .withMongoDbPipeline(
                            this.filter.stream().map(BsonDocument::parse)
                                    .collect(Collectors.toCollection(ArrayList::new))));
        }
        return input.apply("MongoDB Read", readFn)
                .apply("Convert MongoDB Document to Row", ParDo.of(new DoFn<Document, Row>() {
                    @ProcessElement
                    public void processElement(@Element Document input, OutputReceiver<Row> out) {
                        List<Object> vals = new ArrayList<>();
                        mappings.forEach(e -> {
                            LinkedList<String> path = new LinkedList<>(e.getKey());
                            PrimitiveType type = e.getType();
                            Object curr = input;
                            String fieldName = path.removeFirst();
                            while (!path.isEmpty()) {
                                curr = ((Document) curr).get(fieldName);
                            }
                            switch (type) {
                                case BOOLEAN:
                                    vals.add(((Document) curr).getBoolean(fieldName));
                                    break;
                                case DATE:
                                    vals.add(new DateTime(((Document) curr).getDate(fieldName)));
                                    break;
                                case DOUBLE:
                                    vals.add(((Document) curr).getDouble(fieldName));
                                    break;
                                case INTEGER:
                                    vals.add(((Document) curr).getInteger(fieldName));
                                    break;
                                case LONG:
                                    vals.add(((Document) curr).getLong(fieldName));
                                    break;
                                case STRING:
                                    vals.add(((Document) curr).getString(fieldName));
                                    break;
                                case OBJECT_ID:
                                    vals.add(((Document) curr).getObjectId(fieldName).toHexString());
                                    break;
                            }
                        });
                        out.output(Row.withSchema(schema).addValues(vals).build());
                    }
                }));
    }

    public enum PrimitiveType {
        BOOLEAN(Schema.FieldType.of(Schema.TypeName.BOOLEAN)),
        DATE(Schema.FieldType.of(Schema.TypeName.DATETIME)),
        DOUBLE(Schema.FieldType.of(Schema.TypeName.DOUBLE)),
        INTEGER(Schema.FieldType.of(Schema.TypeName.INT32)),
        LONG(Schema.FieldType.of(Schema.TypeName.INT64)),
        STRING(Schema.FieldType.of(Schema.TypeName.STRING)),
        OBJECT_ID(Schema.FieldType.of(Schema.TypeName.STRING));

        private final Schema.FieldType beamType;

        PrimitiveType(Schema.FieldType type) {
            this.beamType = type;
        }
    }

    public class SchemaDefinition implements Serializable {
        private List<String> key;
        private PrimitiveType type;

        public SchemaDefinition() {
        }

        public SchemaDefinition(List<String> key, PrimitiveType type) {
            this.key = key;
            this.type = type;
        }

        public List<String> getKey() {
            return key;
        }

        public void setKey(List<String> key) {
            this.key = key;
        }

        public PrimitiveType getType() {
            return type;
        }

        public void setType(PrimitiveType type) {
            this.type = type;
        }
    }

}
