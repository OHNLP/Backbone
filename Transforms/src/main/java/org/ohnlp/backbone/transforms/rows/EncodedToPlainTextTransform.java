package org.ohnlp.backbone.transforms.rows;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.ohnlp.backbone.api.Transform;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

/**
 * Performs encoded text (e.g. RTF, MSWord doc, etc) to plaintext transforms on a row with a given input and output field.
 * If the input and output fields are the same, the value is replaced.<br/>
 * <br/>
 * Both input and output columns are expected to be of a byte array type, output is guaranteed to be a UTF-8 encoded
 * string represented in byte array format<br/>
 * <br/>
 * Plaintext transform is accomplished using the Apache Tika library, and as such compatibility is congruent with the
 * version of tika used. For more information, please reference the Tika documentation page on supported formats.
 *
 * <p>
 * Expected configuration structure:
 * <pre>
 *     {
 *         "input": "input_column_name",
 *         "output": "output_column_name"
 *     }
 * </pre>
 */
public class EncodedToPlainTextTransform extends Transform {

    private String inputField;
    private String outputField;

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        try {
            this.inputField = config.get("input").asText();
            this.outputField = config.get("output").asText();

        } catch (Throwable t) {
            t.printStackTrace();
        }
    }


    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        if (!this.inputField.equals(this.outputField)) {
            input = input.apply(AddFields.<Row>create().field(this.outputField, Schema.FieldType.STRING));
        }
        return input.apply(ParDo.of(new TikaDecoderFunction(this.inputField, this.outputField)));
    }

    /*
     * The actual DoFn must be in it's own self-contained class due to tika components not being serializable, rather
     * the components should be initialized on each separate executor instance via the Setup annotation
     */
    private static class TikaDecoderFunction extends DoFn<Row, Row> {
        private transient AutoDetectParser parser;
        private transient BodyContentHandler handler;
        private transient Metadata metadata;

        private final String inputField;
        private final String outputField;

        public TikaDecoderFunction(String inputField, String outputField) {
            this.inputField = inputField;
            this.outputField = outputField;
        }

        @Setup
        public void init() {
            this.parser = new AutoDetectParser();
            this.handler = new BodyContentHandler();
            this.metadata = new Metadata();
        }

        @ProcessElement
        public void processElement(@Element Row input, OutputReceiver<Row> output) throws TikaException, SAXException, IOException {
            String encoded = input.getString(inputField);
            parser.parse(new ByteArrayInputStream(encoded.getBytes(StandardCharsets.UTF_8)), handler, metadata);
            String decoded = handler.toString();
            Row out;
            if (!input.getSchema().hasField(inputField)) {
                throw new IllegalArgumentException("Expected field " + inputField + " not found in rtf transform input");
            }
            if (!input.getSchema().hasField(outputField)) { // Output not in schema, compose new row column
                List<Schema.Field> fields = new LinkedList<>(input.getSchema().getFields());
                fields.add(Schema.Field.of(outputField, Schema.FieldType.STRING));
                Schema schema = Schema.of(fields.toArray(new Schema.Field[0]));
                out = Row.withSchema(schema).addValues(input.getValues()).addValue(decoded).build();
            } else {
                // Otherwise just replace in-place
                Row.FieldValueBuilder outBuilder = Row.fromRow(input);
                out = outBuilder.withFieldValue(outputField, decoded).build();
            }
            output.output(out);
        }
    }
}
