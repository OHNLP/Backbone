package org.ohnlp.backbone.io.mongodb;

import org.apache.beam.sdk.io.mongodb.AggregationQuery;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.bson.BsonDocument;
import org.bson.Document;
import org.joda.time.DateTime;
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.components.ExtractToOne;
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
 *         },
 *         "aggregate_pipeline": ["{}", "{}"] // Your MongoDB query function pipeline as a list of JSON-escaped strings
 *     }
 * </pre>
 * <p>
 * For full listing of possible URI formats, please consult <a href="https://www.mongodb.com/docs/manual/reference/connection-string/">MongoDB Documentation</a>
 * <br/>
 * Schema corresponds to the schema of the input document <br/>
 * <br/>
 */
@ComponentDescription(
        name = "Read Records from a MongoDB datasource",
        desc = "Reads Records from a MongoDB Data source. Allowing disk spillover of query sorting is highly recommended. " +
                "A schema corresponding to that of the input documents must be supplied. " +
                "An aggregate pipeline can optionally be supplied as well to act as a filter/preprocessing step."
)
public class MongoDBExtract extends ExtractToOne {
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
            path = "schema",
            desc = "The input schema",
            required = false
    )
    private Schema schema;
    private LinkedList<SchemaDefinition> mappings;
    @ConfigurationProperty(
            path = "aggregate_pipeline",
            desc = "The MongoDB query function pipeline as a list of JSON-escaped strings if applicable. Leave blank otherwise",
            required = false
    )
    private List<String> filter = new ArrayList<>();

    @Override
    public void init() throws ComponentInitializationException {
        // Iterate through injected schema to generate equivalent mappings
        this.mappings = new LinkedList<>();
        LinkedList<String> pathQue = new LinkedList<>();
        for (Schema.Field field : schema.getFields()) {
            parseSchema(field, pathQue);
        }
    }

    @Override
    public List<String> getOutputTags() {
        return Collections.singletonList("MongoDB Records: " + collection);
    }

    @Override
    public Schema calculateOutputSchema() {
        return this.schema;
    }

    @Override
    public PCollection<Row> begin(PBegin input) {
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
                })).setRowSchema(schema);
    }

    private void parseSchema(Schema.Field field, LinkedList<String> pathQue) {
        pathQue.add(field.getName());
        Schema.FieldType type;
        if (field.getType().getTypeName().equals(Schema.TypeName.ROW)) {
            for (Schema.Field f : field.getType().getRowSchema().getFields()) {
                parseSchema(f, pathQue);
            }
            return;
        } else if (field.getType().getTypeName().isCollectionType()) {
            pathQue.add("[]");
            type = field.getType().getCollectionElementType();
            throw new UnsupportedOperationException("Collection types currently not supported");
            // TODO this should be implementable by forcing grouping once collections encountered
        } else if (field.getType().getTypeName().isMapType()) {
            throw new UnsupportedOperationException("Map types currently not supported");
        } else {
            type = field.getType();
        }
        PrimitiveType primType = PrimitiveType.fieldTypeToPrimitiveTypeMap.get(type);
        this.mappings.addLast(new SchemaDefinition(new ArrayList<>(pathQue), primType));
    }

    public enum PrimitiveType {
        BOOLEAN(Schema.FieldType.of(Schema.TypeName.BOOLEAN)),
        DATE(Schema.FieldType.of(Schema.TypeName.DATETIME)),
        DOUBLE(Schema.FieldType.of(Schema.TypeName.DOUBLE)),
        INTEGER(Schema.FieldType.of(Schema.TypeName.INT32)),
        LONG(Schema.FieldType.of(Schema.TypeName.INT64)),
        STRING(Schema.FieldType.of(Schema.TypeName.STRING)),
        OBJECT_ID(Schema.FieldType.of(Schema.TypeName.STRING));

        private static Map<Schema.FieldType, PrimitiveType> fieldTypeToPrimitiveTypeMap = new HashMap<>();

        static {
            for (PrimitiveType type : PrimitiveType.values()) {
                fieldTypeToPrimitiveTypeMap.put(type.beamType, type);
            }
        }

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
