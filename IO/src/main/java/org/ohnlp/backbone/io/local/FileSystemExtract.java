package org.ohnlp.backbone.io.local;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Reads in records from a file system directory.
 *
 * <b>Note:</b> This component is intended for debugging use in local mode only - no guarantees are made about
 * functionality in other environments
 * <p>
 * Expected configuration structure:
 * <pre>
 *     {
 *         "fileSystemPath": "path/to/input/dir/containing/records",
 *         "recordIDField": "name_of_field_in_which_to_store_record_ids",
 *         "recordBodyField": "name_of_field_in_which_to_store_record_contents"
 *     }
 * </pre>
 */
public class FileSystemExtract extends Extract {

    private FileIO.Match fileIterator;
    private String recordIDField;
    private String recordBodyField;
    private Schema rowSchema;

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        this.fileIterator = FileIO.match().filepattern(config.get("fileSystemPath").asText() + "/*");
        this.recordIDField = config.get("recordIDField").asText();
        this.recordBodyField = config.get("recordBodyField").asText();
        List<Schema.Field> fields = new LinkedList<>();
        fields.add(Schema.Field.of(recordIDField, Schema.FieldType.STRING));
        fields.add(Schema.Field.of(recordBodyField, Schema.FieldType.STRING));
        this.rowSchema = Schema.of(fields.toArray(new Schema.Field[0]));
    }

    @Override
    public Schema calculateOutputSchema(Schema input) {
        return this.rowSchema;
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
        PCollection<MatchResult.Metadata> records = this.fileIterator.expand(input);
        return records
                // TODO support recursion
                .apply(FileIO.readMatches().withDirectoryTreatment(FileIO.ReadMatches.DirectoryTreatment.SKIP))
                .apply(ParDo.of(new DoFn<FileIO.ReadableFile, Row>() {
                    @ProcessElement
                    public void processElement(@Element FileIO.ReadableFile input, OutputReceiver<Row> output) throws IOException {
                        String id = input.getMetadata().resourceId().getFilename();
                        String content = input.readFullyAsUTF8String();
                        output.output(Row.withSchema(rowSchema)
                                .withFieldValue(recordIDField, id)
                                .withFieldValue(recordBodyField, content)
                                .build());
                    }
                }));
    }

}
