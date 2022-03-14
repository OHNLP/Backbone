package org.ohnlp.backbone.io.local;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Load;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.util.ArrayList;

/**
 * Writes Parquet Formatted Files to Directory
 *
 * Expected Configuration:
 * {
 *     "fileSystemPath": "path/to/write/to",
 *     "fields": ["optional", "list", "of", "columns", "to", "write"]
 * }
 */
public class ParquetLoad extends Load {
    private String dir;
    private ArrayList<String> fields;

    private transient Schema beamSchema;
    private transient org.apache.avro.Schema avroSchema;

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        this.dir = config.get("fileSystemPath").asText();
        this.fields = new ArrayList<>();
        if (config.has("fields")) {
            config.get("fields").forEach((f) -> {
                String field = f.asText();
                fields.add(field);
            });
        }

    }

    @Override
    public PDone expand(PCollection<Row> input) {
        input.apply("Subselect Columns and Convert to Avro Format", ParDo.of(new DoFn<Row, GenericRecord>() {
                    @ProcessElement
                    public void processElement(@Element Row input, OutputReceiver<GenericRecord> output) {
                        if (beamSchema == null) {
                            // Assume homogeneous schema
                            Schema sourceSchema = input.getSchema();
                            if (!fields.isEmpty()) {
                                Schema.Builder targetBeamSchemaBuilder = org.apache.beam.sdk.schemas.Schema.builder();
                                for (String f : fields) {
                                    targetBeamSchemaBuilder.addField(f, sourceSchema.getField(f).getType());
                                }
                                beamSchema = targetBeamSchemaBuilder.build();
                            } else {
                                beamSchema = sourceSchema;
                            }
                            avroSchema = AvroUtils.toAvroSchema(beamSchema);
                        }
                        output.output(AvroUtils.toGenericRecord(input, avroSchema));
                    }
                }))
                .apply("Write to Filesystem", FileIO
                        .<GenericRecord>write()
                        .via(ParquetIO.sink(avroSchema))
                        .to(this.dir)
                );
        return PDone.in(input.getPipeline());
    }
}
