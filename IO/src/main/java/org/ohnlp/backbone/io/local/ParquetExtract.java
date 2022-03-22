package org.ohnlp.backbone.io.local;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.util.Locale;

/**
 * Reads Parquet Formatted Files From Directory
 *
 * Expected Configuration:
 * {
 *     "fileSystemPath": "path/to/write/to",
 *     "recordName": "Schema Record Name",
 *     "recordNamespace": "Schema Record Namespace",
 *     "schema": {
 *         "fieldName": "fieldType"
 *     }
 * }
 *
 * For a list of field types, please consult https://avro.apache.org/docs/current/spec.html#schema_primitive.
 * Primitive types are the only types currently supported.
 */
public class ParquetExtract extends Extract {
    private String dir;
    private transient Schema schema;
    private org.apache.beam.sdk.schemas.Schema beamSchema;

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        this.dir = config.get("fileSystemPath").asText();
        SchemaBuilder.FieldAssembler<Schema> avroSchemaBuilder = SchemaBuilder.builder()
                .record(config.get("recordName").asText())
                .namespace(config.get("recordNamespace").asText())
                .fields();
        config.get("schema").fields().forEachRemaining((e) -> {
            String field = e.getKey();
            avroSchemaBuilder.name(field).type(e.getValue().asText()).noDefault();
        });
        schema = avroSchemaBuilder.endRecord();
        beamSchema = AvroUtils.toBeamSchema(schema);
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
        return input
                .apply("Parquet Read", ParquetIO
                        .read(this.schema)
                        .from(this.dir))
                .setCoder(AvroCoder.of(GenericRecord.class, schema))
                .apply("Convert to Beam Row", ParDo.of(new AvroToBeam(beamSchema)));
    }

    private class AvroToBeam extends DoFn<GenericRecord, Row> {

        private org.apache.beam.sdk.schemas.Schema beamSchema;

        public AvroToBeam(org.apache.beam.sdk.schemas.Schema beamSchema) {
            this.beamSchema = beamSchema;
        }

        @ProcessElement
        public void processElement(@Element GenericRecord input, OutputReceiver<Row> output) {
            output.output(AvroUtils.toBeamRowStrict(input, beamSchema));
        }
    }
}
