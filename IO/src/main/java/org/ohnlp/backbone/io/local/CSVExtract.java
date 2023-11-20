package org.ohnlp.backbone.io.local;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.joda.time.ReadableDateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.components.ExtractToMany;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.io.Repartition;

import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

@ComponentDescription(
        name = "Read CSVs from Filesystem",
        desc = "Reads CSVs from a Directory. If multiple files exist in the directory, " +
                "assumes they all follow the same schema and reads in parallel"
)
public class CSVExtract extends ExtractToMany {
    @ConfigurationProperty(
            path = "fileSystemPath",
            desc = "The file system path to read from"
    )
    private String dir;
    @ConfigurationProperty(
            path = "firstRowIsHeader",
            desc = "Whether the first row is a header column"
    )
    private boolean skipFirstRow;

    @ConfigurationProperty(
            path = "fanout",
            desc = "Whether to reshuffle records after read. Defaults to false. Set to true if fanout is needed (i.e., if your individual CSV files are very large)",
            required = false
    )
    private boolean fanout = false;

    @ConfigurationProperty(
            path = "schema",
            desc = "CSV File Schema"
    )
    private Schema schema;

    private Schema errorSchema = Schema.of(
            Schema.Field.nullable("Record", Schema.FieldType.STRING),
            Schema.Field.nullable("Exception", Schema.FieldType.STRING)
    );

    @Override
    public void init() throws ComponentInitializationException {
    }

    @Override
    public PCollectionRowTuple expand(PBegin input) {

        PCollection<String> fileURIs = input.apply("Scan Input Directory for Partitioned Files", Create.of(Arrays.stream(Objects.requireNonNull(new File(dir).listFiles())).map(f -> f.toURI().toString()).collect(Collectors.toList()))).setCoder(StringUtf8Coder.of());
        PCollectionTuple readColls = fileURIs.apply("Read CSV Records and Map to Rows", ParDo.of(new DoFn<String, Row>() {
            private ObjectMapper om;
            private String[] header;
            private Map<Schema.TypeName, SerializableFunction<String, Object>> typeResolvers;

            @Setup
            public void init() {
                this.header = schema.getFields().stream().map(Schema.Field::getName).toArray(String[]::new);
                this.typeResolvers = new HashMap<>();
                this.typeResolvers.put(Schema.TypeName.BYTE, Byte::parseByte);
                this.typeResolvers.put(Schema.TypeName.BYTES, s -> {
                    throw new UnsupportedOperationException("Cannot deserialize byte arrays from CSV");
                });
                this.typeResolvers.put(Schema.TypeName.DATETIME, s -> ISODateTimeFormat.dateTimeParser().parseDateTime(s));
                this.typeResolvers.put(Schema.TypeName.DECIMAL, BigDecimal::new);
                this.typeResolvers.put(Schema.TypeName.DOUBLE, Double::parseDouble);
                this.typeResolvers.put(Schema.TypeName.FLOAT, Float::parseFloat);
                this.typeResolvers.put(Schema.TypeName.INT16, Short::parseShort);
                this.typeResolvers.put(Schema.TypeName.INT32, Integer::parseInt);
                this.typeResolvers.put(Schema.TypeName.INT64, Long::parseLong);
                this.typeResolvers.put(Schema.TypeName.STRING, s -> s);
                this.om = new ObjectMapper();
            }

            private SerializableFunction<String, Object> getFieldDeserializationFunction(Schema.FieldType type) {
                if (type.getTypeName().isPrimitiveType()) {
                    SerializableFunction<String, Object> func = this.typeResolvers.get(type.getTypeName());
                    if (func == null) {
                        throw new UnsupportedOperationException("Deserialization of Type " + type.getTypeName().name() + " from CSV is not Supported");
                    }
                    return func;
                } else { // Treat as a JSON Object and parse back using type info
                    return new SerializableFunction<String, Object>() {
                        @Override
                        public Object apply(String input) {
                            try {
                                JsonNode json = om.readTree(input);
                                return parseJSONFromField(json, type);
                            } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };
                }
            }

            @ProcessElement
            public void processFile(ProcessContext pc) {
                URI fileURI = URI.create(pc.element());
                try (Reader reader = new FileReader(new File(fileURI))) {
                    CSVFormat format = CSVFormat.EXCEL.withHeader(this.header).withSkipHeaderRecord(skipFirstRow).withNullString("");
                    CSVParser parser = format.parse(reader);
                    parser.iterator().forEachRemaining(r -> {
                        try {
                            Row.Builder rowBuilder = Row.withSchema(schema);
                            for (Schema.Field field : schema.getFields()) {
                                String textValue = r.get(field.getName());
                                if (textValue == null) {
                                    rowBuilder.addValue(null);
                                } else {
                                    SerializableFunction<String, Object> func = this.getFieldDeserializationFunction(field.getType());
                                    rowBuilder.addValue(func.apply(textValue));
                                }
                            }
                            pc.output(new TupleTag<>("CSV Records"), rowBuilder.build());
                        } catch (Throwable t) {
                            pc.output(new TupleTag<>("Errored Records"), Row.withSchema(errorSchema).addValue(r.toString()).addValue(ExceptionUtils.getStackTrace(t)).build());
                        }
                    });
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }
        }).withOutputTags(new TupleTag<>("CSV Records"), TupleTagList.of(new TupleTag<>("Errored Records"))));
        PCollection<Row> read = readColls.get("CSV Records");
        read.setRowSchema(this.schema);
        if (this.fanout) {
            read = read.apply("Break Fusion", Repartition.of());
        }
        PCollectionRowTuple ret = PCollectionRowTuple.of("CSV Records", read).and("Errored Records", readColls.get("Errored Records"));
        // Set Coders
        ret.get("CSV Records").setRowSchema(this.schema);
        ret.get("Errored Records").setRowSchema(this.errorSchema);
        return ret;
    }

    private Object parseJSONFromField(JsonNode json, Schema.FieldType type) {
        switch (type.getTypeName()) {
            case BYTE:
                return ((Integer)json.asInt()).byteValue();
            case INT16:
                return ((Integer)json.asInt()).shortValue();
            case INT32:
                return json.asInt();
            case INT64:
                return json.asLong();
            case DECIMAL:
                return json.decimalValue();
            case FLOAT:
                return json.decimalValue().floatValue();
            case DOUBLE:
                return json.decimalValue().doubleValue();
            case STRING:
                return json.asText();
            case DATETIME:
                return ISODateTimeFormat.dateTimeParser().parseDateTime(json.asText());
            case BOOLEAN:
                return json.booleanValue();
            case BYTES:
                try {
                    return json.binaryValue();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            case ARRAY:
            case ITERABLE: {
                List<Object> ret = new ArrayList<>();
                for (JsonNode node : json) {
                    ret.add(parseJSONFromField(node, type.getCollectionElementType()));
                }
                return ret;
            }
            case MAP: {
                Map<String, Object> ret = new HashMap<>(); // Only string->object mappings supported
                json.fields().forEachRemaining(f -> {
                    ret.put(f.getKey(), parseJSONFromField(f.getValue(), type.getMapValueType()));
                });
                return ret;
            }
            case ROW: {
                Schema val = type.getRowSchema();
                List<Object> values = new ArrayList<>();
                val.getFields().forEach(f -> {
                    JsonNode childNode = json.get(f.getName());
                    Object add = null;
                    if (childNode != null) {
                        add = parseJSONFromField(childNode, f.getType());
                    }
                    values.add(add);
                });
                return Row.withSchema(val).addValues(values).build();
            }
            default:
                throw new UnsupportedOperationException("Deserialization of " + type.getTypeName().name() + " is not currently supported");
        }
    }


    @Override
    public List<String> getOutputTags() {
        return Arrays.asList("CSV Records", "Errored Records");
    }

    @Override
    public Map<String, Schema> calculateOutputSchema(Map<String, Schema> input) {
        return Map.of(
                "CSV Records", this.schema,
                "Errored Records", this.errorSchema
        );
    }
}
