package org.ohnlp.backbone.api.components.xlang.python;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.*;
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
        PCollection<String> pythonInput = ToJson.<Row>of().expand(inputColl);
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
                    JsonToRow.withSchema(getComponentSchema().get(output))));
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
}
