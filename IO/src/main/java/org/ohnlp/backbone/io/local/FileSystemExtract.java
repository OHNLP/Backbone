package org.ohnlp.backbone.io.local;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
@ComponentDescription(
        name = "Read Records from Filesystem",
        desc = "Reads Records from a Directory, populating recordIDField with the file name and recordBodyField with the raw textual contents"
)
public class FileSystemExtract extends Extract {

    private FileIO.Match fileIterator;
    @ConfigurationProperty(
            path = "fileSystemPath",
            desc = "The file system path to read from"
    )
    private String dir;
    @ConfigurationProperty(
            path = "recordIDField",
            desc = "The name of the column into which file names should be placed"
    )
    private String recordIDField;
    @ConfigurationProperty(
            path = "recordBodyField",
            desc = "The name of the column into which file contents should be placed"
    )
    private String recordBodyField;
    private Schema rowSchema;

    @Override
    public void init() throws ComponentInitializationException {
        this.fileIterator = FileIO.match().filepattern(dir + "/*");
        List<Schema.Field> fields = new LinkedList<>();
        fields.add(Schema.Field.of(recordIDField, Schema.FieldType.STRING));
        fields.add(Schema.Field.of(recordBodyField, Schema.FieldType.STRING));
        this.rowSchema = Schema.of(fields.toArray(new Schema.Field[0]));
    }

    @Override
    public List<String> getOutputTags() {
        return Collections.singletonList("Filesystem Records");
    }

    @Override
    public Map<String, Schema> calculateOutputSchema(Map<String, Schema> input) {
        return Collections.singletonMap(getOutputTags().get(0), this.rowSchema);
    }

    @Override
    public PCollectionRowTuple expand(PBegin input) {
        PCollection<MatchResult.Metadata> records = this.fileIterator.expand(input);
        return PCollectionRowTuple.of(getOutputTags().get(0),
                records
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
                })
                ).setRowSchema(this.rowSchema)
        );
    }

}
