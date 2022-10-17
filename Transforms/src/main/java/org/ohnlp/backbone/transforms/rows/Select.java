package org.ohnlp.backbone.transforms.rows;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Transform;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.util.ArrayList;
import java.util.List;

public class Select extends Transform {

    private List<String> selectedFields;
    private Schema outSchema;


    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        this.selectedFields = new ArrayList<>();
        if (config.has("select")) {
            for (JsonNode fieldName : config.get("select")) {
                selectedFields.add(fieldName.asText());
            }
        }
    }

    @Override
    public Schema calculateOutputSchema(Schema input) {
        List<Schema.Field> outputSchemaFields = new ArrayList<>();
        for (String fieldName : selectedFields) {
            outputSchemaFields.add(input.getField(fieldName));
        }
        this.outSchema = Schema.of(outputSchemaFields.toArray(new Schema.Field[0]));
        return outSchema;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        return input.apply("Subset Columns", ParDo.of(new DoFn<Row, Row>() {
            @ProcessElement
            public void process(ProcessContext c) {
                Row input = c.element();
                // And now just map the values
                List<Object> outputValues = new ArrayList<>();
                for (String s : selectedFields) {
                    outputValues.add(input.getValue(s));
                }
                c.output(Row.withSchema(outSchema).addValues(outputValues).build());
            }
        }));
    }
}
