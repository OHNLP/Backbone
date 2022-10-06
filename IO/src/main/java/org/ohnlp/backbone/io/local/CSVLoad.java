package org.ohnlp.backbone.io.local;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Load;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.io.local.encodings.RowHeaderToCSVEncoding;
import org.ohnlp.backbone.io.local.encodings.RowValueToCSVEncoding;
import org.ohnlp.backbone.io.local.functions.FileSystemLoadTransform;

import java.util.ArrayList;
import java.util.List;


/**
 * Writes records to a file system directory in CSV format
 *
 * <b>Note:</b> This component is intended for debugging use in local mode only - no guarantees are made about
 * functionality in other environments
 * <p>
 * Expected configuration structure:
 * <pre>
 *     {
 *         "fileSystemPath": "path/to/output/dir/to/write/records",
 *         "writeHeader": true|false,
 *         "fields": ["optional", "array", "of", "fields", "to", "include", "defaults", "all"]
 *     }
 * </pre>
 */
public class CSVLoad extends Load {
    private String workingDir;
    private boolean writeHeader;
    private List<String> fields;

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        this.workingDir = config.get("fileSystemPath").asText();
        this.writeHeader = config.get("writeHeader").asBoolean(true);
        this.fields = new ArrayList<>();
        if (config.has("fields")) {
            for (JsonNode field : config.get("fields")) {
                fields.add(field.asText());
            }
        }
    }

    @Override
    public PDone expand(PCollection<Row> input) {
        return new FileSystemLoadTransform(
                workingDir,
                ".csv",
                this.writeHeader ? new RowHeaderToCSVEncoding() : null,
                new RowValueToCSVEncoding(),
                fields
        ).expand(input);
    }
}
