package org.ohnlp.backbone.io.local;

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
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.util.Map;

/**
 * Reads Parquet Formatted Files From Directory
 *
 * Expected Configuration:
 * {
 *     "fileSystemPath": "path/to/read/from/*",
 *     "recordName": "Schema Record Name",
 *     "recordNamespace": "Schema Record Namespace",
 *     "schema": {
 *         "fieldName": "fieldType"
 *     }
 * }
 */
public class ParquetExtract extends Extract {
    @ConfigurationProperty(
            path = "fileSystemPath",
            desc = "The file system path containing parquet records"
    )
    private String dir;
    @ConfigurationProperty(
            path = "recordName",
            desc = "Parquet Schema Record Name"
    )
    private String recordName;
    @ConfigurationProperty(
            path = "recordNamespace",
            desc = "Parquet Schema Record Namespace"
    )
    private String recordNamespace;

    private transient Schema schema;

    @ConfigurationProperty(
            path = "schema",
            desc = "The record schema"
    )
    private org.apache.beam.sdk.schemas.Schema beamSchema;

    @Override
    public void init() throws ComponentInitializationException {
        schema = AvroUtils.toAvroSchema(beamSchema, recordName, recordNamespace);
    }

    @Override
    public org.apache.beam.sdk.schemas.Schema calculateOutputSchema(org.apache.beam.sdk.schemas.Schema input) {
        return this.beamSchema;
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
