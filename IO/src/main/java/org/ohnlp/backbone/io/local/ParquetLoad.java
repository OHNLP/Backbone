package org.ohnlp.backbone.io.local;

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
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.annotations.InputColumnProperty;
import org.ohnlp.backbone.api.components.LoadFromOne;
import org.ohnlp.backbone.api.config.InputColumn;
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
@ComponentDescription(
        name = "Write Records into Filesystem as Parquet",
        desc = "Writes parquet records into a specified filesystem. One file will be created per partition/batch. " +
                "An optional list of columns to subset for output can be supplied."
)
public class ParquetLoad extends LoadFromOne {

    @ConfigurationProperty(
            path = "fileSystemPath",
            desc = "The path to write to"
    )
    private String dir;
    @ConfigurationProperty(
            path = "fields",
            desc = "An optional list/subset of the columns to write. Leave blank for all",
            required = false
    )
    @InputColumnProperty
    private ArrayList<InputColumn> fields = new ArrayList<>();

    private transient Schema beamSchema;

    @Override
    public void init() throws ComponentInitializationException {
    }


    @Override
    public PDone expand(PCollection<Row> input) {
        beamSchema = input.getSchema();
        // TODO seems subselection functionality is not present?
        input.apply("Subselect Columns and Convert to Avro Format", ParDo.of(new DoFn<Row, GenericRecord>() {
                    @ProcessElement
                    public void processElement(@Element Row input, OutputReceiver<GenericRecord> output) {
                        output.output(AvroUtils.toGenericRecord(input, AvroUtils.toAvroSchema(beamSchema)));
                    }
                }))
                .apply("Write to Filesystem", FileIO
                        .<GenericRecord>write()
                        .via(ParquetIO.sink(AvroUtils.toAvroSchema(beamSchema)))
                        .to(this.dir)
                );
        return PDone.in(input.getPipeline());
    }
}
