package org.ohnlp.backbone.io.local;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Load;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.io.local.encodings.RowToJSONEncoding;
import org.ohnlp.backbone.io.local.functions.FileSystemLoadTransform;

/**
 * Writes records to a file system directory in JSON Lines format (JSONs w/ newline delimitation between records)
 *
 * <b>Note:</b> This component is intended for debugging use in local mode only - no guarantees are made about
 * functionality in other environments
 * <p>
 * Expected configuration structure:
 * <pre>
 *     {
 *         "fileSystemPath": "path/to/output/dir/to/write/records"
 *     }
 * </pre>
 */
public class JSONLLoad extends Load {

    private String workingDir;

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        this.workingDir = config.get("fileSystemPath").asText();
    }

    @Override
    public PDone expand(PCollection<Row> input) {
        return new FileSystemLoadTransform(
                workingDir,
                ".jsonl",
                null, new RowToJSONEncoding()).expand(input);
    }
}
