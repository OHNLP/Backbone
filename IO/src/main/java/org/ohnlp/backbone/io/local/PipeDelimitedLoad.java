package org.ohnlp.backbone.io.local;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.annotations.InputColumnProperty;
import org.ohnlp.backbone.api.components.LoadFromOne;
import org.ohnlp.backbone.api.config.InputColumn;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.io.local.encodings.RowHeaderToCSVEncoding;
import org.ohnlp.backbone.io.local.encodings.RowHeaderToPipeDelimitedEncoding;
import org.ohnlp.backbone.io.local.encodings.RowValueToCSVEncoding;
import org.ohnlp.backbone.io.local.encodings.RowValueToPipeDelimitedEncoding;
import org.ohnlp.backbone.io.local.functions.FileSystemLoadTransform;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


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
@ComponentDescription(
        name = "Write Records into Filesystem as Pipe-Delimited File",
        desc = "Writes records to a file system directory in Pipe-Delimited File format. One file is created per partition. " +
                "This component is intended for debugging use in local mode only - no guarantees are made about " +
                "functionality in other environments"
)
public class PipeDelimitedLoad extends LoadFromOne {
    @ConfigurationProperty(
            path = "fileSystemPath",
            desc = "The file system path to write to"
    )
    private String workingDir;
    @ConfigurationProperty(
            path = "writeHeader",
            desc = "Whether the header should be appended to output",
            required = false
    )
    private boolean writeHeader = true;
    @ConfigurationProperty(
            path = "fields",
            desc = "An optional array of columns to include in output. Leave blank for all",
            required = false
    )
    @InputColumnProperty
    private List<InputColumn> fields = new ArrayList<>();

    @Override
    public void init() throws ComponentInitializationException {
    }

    @Override
    public PDone expand(PCollection<Row> input) {
        return new FileSystemLoadTransform(
                workingDir,
                ".csv",
                this.writeHeader ? new RowHeaderToPipeDelimitedEncoding() : null,
                new RowValueToPipeDelimitedEncoding(),
                fields.stream().map(InputColumn::getSourceColumnName).collect(Collectors.toList())
        ).expand(input);
    }
}
