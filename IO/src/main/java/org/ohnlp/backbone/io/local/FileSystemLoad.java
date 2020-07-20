package org.ohnlp.backbone.io.local;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.base.Joiner;
import org.apache.commons.text.StringEscapeUtils;
import org.ohnlp.backbone.api.Load;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Reads in records to a file system directory.
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
public class FileSystemLoad extends Load {

    private FileIO.Write<Void, Row> writer;

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        this.writer = FileIO.<Row>write()
                .via(new CSVSink())
                .to(config.get("fileSystemPath").asText())
                .withPrefix("backbone-out-")
                .withSuffix(".csv");
    }

    @Override
    public PDone expand(PCollection<Row> input) {

        input.apply(writer);
        return PDone.in(input.getPipeline());
    }

    private static class CSVSink implements FileIO.Sink<Row> {
        private PrintWriter writer;
        private boolean wroteHeader;

        public void open(WritableByteChannel channel) throws IOException {
            writer = new PrintWriter(Channels.newOutputStream(channel));
            wroteHeader = false;
        }

        public void write(Row element) throws IOException {
            if (!wroteHeader) {
                writer.println(element.getSchema().getFieldNames().stream().map(StringEscapeUtils::escapeCsv).collect(Collectors.joining(",")));
                wroteHeader = true;
            }
            writer.println(Joiner.on(",").join(element.getValues().stream().map(o -> StringEscapeUtils.escapeCsv(o.toString())).collect(Collectors.toList())));
        }

        public void flush() throws IOException {
            writer.flush();
        }
    }
}
