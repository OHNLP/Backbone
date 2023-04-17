package org.ohnlp.backbone.transforms.rows;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.annotations.InputColumnProperty;
import org.ohnlp.backbone.api.components.OneToOneTransform;
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.config.InputColumn;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.util.ArrayList;
import java.util.List;

@ComponentDescription(
        name = "Select/Subset Columns",
        desc = "Performs operation akin to SQL Select, which subsets only the specified list of column names to the output."
)
public class Select extends OneToOneTransform {

    @ConfigurationProperty(
            path = "select",
            desc = "List of columns to select"
    )
    @InputColumnProperty(
            sourceTags = {"*"},
            allowableTypes = {}
    )
    private List<InputColumn> selectedFields;
    private Schema outSchema;


    @Override
    public void init() throws ComponentInitializationException {
    }

    @Override
    public Schema calculateOutputSchema(Schema input) {
        List<Schema.Field> outputSchemaFields = new ArrayList<>();
        for (InputColumn fieldName : selectedFields) {
            outputSchemaFields.add(input.getField(fieldName.getSourceColumnName()));
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
                for (InputColumn s : selectedFields) {
                    outputValues.add(input.getValue(s.getSourceColumnName()));
                }
                c.output(Row.withSchema(outSchema).addValues(outputValues).build());
            }
        }));
    }
}
