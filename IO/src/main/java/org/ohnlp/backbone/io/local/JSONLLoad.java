package org.ohnlp.backbone.io.local;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Load;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.io.local.encodings.RowToJSONEncoding;
import org.ohnlp.backbone.io.local.functions.FileSystemLoadTransform;

import java.util.ArrayList;
import java.util.List;

/**
 * Writes records to a file system directory in JSON Lines format (JSONs w/ newline delimitation between records)
 *
 * <b>Note:</b> This component is intended for debugging use in local mode only - no guarantees are made about
 * functionality in other environments
 * <p>
 * Expected configuration structure:
 * <pre>
 *     {
 *         "fileSystemPath": "path/to/output/dir/to/write/records",
 *         "fields": ["optional", "array", "of", "fields", "to", "include", "defaults", "all"]
 *     }
 * </pre>
 */
public class JSONLLoad extends Load {

    @ConfigurationProperty(
            path = "fileSystemPath",
            desc = "The path to write to"
    )
    private String workingDir;
    @ConfigurationProperty(
            path = "fields",
            desc = "An optional list/subset of the columns to write. Leave blank for all",
            required = false
    )
    private List<String> fields = new ArrayList<>();

    @Override
    public void init() throws ComponentInitializationException {


    }

    @Override
    public PDone expand(PCollection<Row> input) {
        return new FileSystemLoadTransform(
                workingDir,
                ".jsonl",
                null,
                new RowToJSONEncoding(),
                fields).expand(input);
    }
}
