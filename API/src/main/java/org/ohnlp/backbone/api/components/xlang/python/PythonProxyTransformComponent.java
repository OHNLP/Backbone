package org.ohnlp.backbone.api.components.xlang.python;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.joda.time.DateTime;
import org.ohnlp.backbone.api.components.SchemaInitializable;
import org.ohnlp.backbone.api.components.SingleInputComponent;
import org.ohnlp.backbone.api.components.TransformComponent;
import org.ohnlp.backbone.api.components.XLangComponent;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.api.util.SchemaConfigUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class PythonProxyTransformComponent extends TransformComponent implements XLangComponent, SingleInputComponent, SchemaInitializable {

    private String config;
    private String entryPoint;
    private String bundleName;
    private PythonBackbonePipelineComponent proxiedComponent;
    @Override
    public void injectConfig(JsonNode config) {
        try {
            this.bundleName = config.get("python_bundle_name").asText();
            this.entryPoint = config.get("python_entry_point").asText();
            this.config = new ObjectMapper().writer().writeValueAsString(config);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void init() throws ComponentInitializationException {
        try {
            PythonBridge<PythonBackbonePipelineComponent> python = new PythonBridge<>(this.bundleName, this.entryPoint, PythonBackbonePipelineComponent.class);
            python.startBridge();
            this.proxiedComponent = python.getPythonEntryPoint();
            this.proxiedComponent.init(this.config);
        } catch (IOException e) {
            throw new ComponentInitializationException(new RuntimeException("Error initializing python bridge", e));
        }
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
        Map<String, PCollection<Row>> collMap = input.getAll();
        PCollection<Row> inputColl = collMap.get(collMap.keySet().toArray(new String[0])[0]);
        // Get a proxied python DoFn that handles bridge setup on executor nodes, and pass it initialized driver configs
        // as well
        PythonProxyDoFn proxiedDoFn = new PythonProxyDoFn(
                this.bundleName,
                this.entryPoint,
                this.proxiedComponent.to_do_fn_config());
        PCollection<String> pythonInput = inputColl.apply("Python" + this.entryPoint + ": Convert Java Rows to JSON for Python Transfer",
                ParDo.of(new RowToJson(inputColl.getSchema())));
        // Figure out what output tags are actually present and construct relevant tag lists
        List<TupleTag<String>> outputs = this.getOutputTags().stream().map(s -> new TupleTag<String>(s)).collect(Collectors.toList());
        TupleTag<String> firstTag = outputs.get(0);
        outputs.remove(0);
        TupleTagList list = TupleTagList.empty();
        for (TupleTag<String> output : outputs) {
            list = list.and(output);
        }
        // Actually call the proxied python DoFn and return as a PCollectionTuple
        PCollectionTuple pythonOutStringJSON = pythonInput.apply(
                "Python: " + this.entryPoint,
                ParDo.of(proxiedDoFn).withOutputTags(
                        firstTag,
                        list
                )
        );
        // Map individual tag outputs back to Beam Rows
        PCollectionRowTuple rowRet = PCollectionRowTuple.empty(pythonOutStringJSON.getPipeline());
        for (String output : getOutputTags()) {
            rowRet = rowRet.and(output, pythonOutStringJSON.<String>get(output).apply("Convert Python Rows to Java Rows",
                    ParDo.of(new JsonToRow(getComponentSchema().get(output)))));
        }
        return rowRet;
    }

    @Override
    public List<String> getInputTags() {
        return Collections.singletonList(proxiedComponent.get_input_tag());
    }

    @Override
    public List<String> getOutputTags() {
        return proxiedComponent.get_output_tags();
    }

    @Override
    public Map<String, Schema> calculateOutputSchema(Map<String, Schema> input) {
        // Because the Schema type does not exist in python's version of Beam, convert to JSON here
        Map<String, PythonSchema> inputSchemas = new HashMap<>();
        final ObjectMapper om = new ObjectMapper();
        input.forEach((tag, schema) -> {
            String jsonSchema = null;
            try {
                jsonSchema = om.writeValueAsString(SchemaConfigUtils.schemaToJSON(schema));
            } catch (JsonProcessingException ex) {
                throw new RuntimeException("Error parsing Schema", ex);
            }
            inputSchemas.put(tag, this.proxiedComponent.python_schema_from_json_string(jsonSchema));
        });
        // Perform the actual output schema derivation in the proxied python component
        Map<String, PythonSchema> jsonifiedOutputSchema = this.proxiedComponent.calculate_output_schema(inputSchemas);
        // And map this back to java Schemas
        Map<String, Schema> outputSchema = new HashMap<>();
        jsonifiedOutputSchema.forEach((tag, schema) -> {
            try {
                outputSchema.put(tag, SchemaConfigUtils.jsonToSchema(om.readTree(this.proxiedComponent.json_string_from_python_schema(schema))));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Malformed python output schema", e);
            }
        });
        return outputSchema;
    }

    public static class RowToJson extends DoFn<Row, String> {
        public Schema inputSchema;
        private ObjectMapper om;
        private ObjectWriter ow;
        private JsonNode schemaJson;

        public RowToJson(Schema inputSchema) {
            this.inputSchema = inputSchema;
        }

        @StartBundle
        public void init() {
            this.om = new ObjectMapper().registerModule(new JodaModule());
            this.ow = this.om.writer();
            this.schemaJson = SchemaConfigUtils.schemaToJSON(this.inputSchema);
        }

        @ProcessElement
        public void process(ProcessContext pc) {
            ObjectNode ret = JsonNodeFactory.instance.objectNode();
            ret.set("schema", schemaJson);
            Row r = pc.element();
            if (r == null) {
                return;
            }
            ret.set("contents", this.parseRow(this.inputSchema, r));
            try {
                pc.output(ow.writeValueAsString(ret));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        public JsonNode parseRow(Schema schema, Row r) {
            ObjectNode ret = JsonNodeFactory.instance.objectNode();
            schema.getFields().forEach(f -> {
                ret.set(f.getName(), parseFieldContents(f.getType(), r.getValue(f.getName())));
            });
            return ret;
        }

        public JsonNode parseFieldContents(Schema.FieldType field, Object input) {
            if (field.getTypeName().isCollectionType()) {
                if (!(input instanceof Collection)) {
                    throw new IllegalArgumentException("Schema mismatch, expected collection received type " + input.getClass().getName());
                }
                ArrayNode ret = JsonNodeFactory.instance.arrayNode(((Collection<?>)input).size());

                for (Object child : ((Collection<?>)input)) {
                    JsonNode childNode = parseFieldContents(Objects.requireNonNull(field.getCollectionElementType()), child);
                    ret.add(childNode);
                }
                return ret;
            } else if (field.getTypeName().isLogicalType()) {
                if (!(input instanceof Row)) {
                    throw new IllegalArgumentException("Schema mismatch, expected embedded row received type " + input.getClass().getName());
                }
                return parseRow(Objects.requireNonNull(field.getRowSchema()), (Row) input);
            } else {
                return this.om.valueToTree(input);
            }
        }
    }

    public static class JsonToRow extends DoFn<String, Row> {

        private final Schema schema;
        private ObjectMapper om;

        public JsonToRow(Schema schema) {
            this.schema = schema;
        }

        @StartBundle
        public void init() {
            this.om = new ObjectMapper().registerModule(new JodaModule());
        }

        @ProcessElement
        public void process(ProcessContext pc) throws JsonProcessingException {
            JsonNode rowJSON = this.om.readTree(pc.element());
            JsonNode rowContents = rowJSON.get("contents");
            pc.output(parseRowFromJSONWithSchema(this.schema, rowContents));
        }

        private Row parseRowFromJSONWithSchema(Schema rowSchema, JsonNode rowContents) {
            List<Object> values = new ArrayList<>();
            for (Schema.Field f : rowSchema.getFields()) {
                if (rowContents.hasNonNull(f.getName())) {
                    Object val = parseFieldValue(f.getType(), rowContents.get(f.getName()));
                    values.add(val);
                } else {
                    values.add(null);
                }
            }
            return Row.withSchema(rowSchema).addValues(values).build();
        }

        private Object parseFieldValue(Schema.FieldType type, JsonNode json) {
            if (type.getTypeName().isLogicalType()) {
                Schema childSchema = type.getRowSchema();
                return parseRowFromJSONWithSchema(Objects.requireNonNull(childSchema), json);
            } else if (type.getTypeName().isCollectionType()) {
                Schema.FieldType elementType = type.getCollectionElementType();
                List<Object> elements = new ArrayList<>();
                // JSON should be an ArrayNode, so just iterate in order over contents
                json.forEach(child -> {
                    elements.add(parseFieldValue(Objects.requireNonNull(elementType), child));
                });
                return elements;
            } else {
                switch (type.getTypeName()) {
                    case BYTE:
                        return (byte)json.intValue();
                    case INT16:
                        return (short)json.intValue();
                    case INT32:
                        return json.intValue();
                    case INT64:
                        return json.longValue();
                    case DECIMAL:
                        return json.decimalValue();
                    case FLOAT:
                        return json.floatValue();
                    case DOUBLE:
                        return json.doubleValue();
                    case STRING:
                        return json.asText();
                    case DATETIME:
                        return DateTime.parse(json.asText());
                    case BOOLEAN:
                        return json.booleanValue();
                    default:
                        throw new IllegalArgumentException("Attempted to deserialize value of type " + type.getTypeName() + " as a simple/primitive value");
                }
            }
        }
    }
}
