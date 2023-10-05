package org.ohnlp.backbone.io.local.functions;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.io.local.encodings.RowToTextEncoding;

import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class FileSystemLoadTransform extends PTransform<PCollection<Row>, PDone> {

    private final String outputDir;
    private final String ext;
    private final RowToTextEncoding headerEncoding;
    private final RowToTextEncoding contentEncoding;
    private final List<String> fields;

    private transient boolean wroteHeader;

    public FileSystemLoadTransform(String outputDir, String ext, RowToTextEncoding headerEncoding, RowToTextEncoding contentEncoding) {
        this(outputDir, ext, headerEncoding, contentEncoding, Collections.emptyList());
    }

    public FileSystemLoadTransform(String outputDir, String ext, RowToTextEncoding headerEncoding, RowToTextEncoding contentEncoding, List<String> fields) {
        this.outputDir = outputDir;
        this.ext = ext;
        this.headerEncoding = headerEncoding;
        this.contentEncoding = contentEncoding;
        this.fields = fields;
    }

    @Override
    public PDone expand(PCollection<Row> input) {
        input.apply("File System Load", ParDo.of(new DoFn<Row, Void>() {
            private transient Writer writer;
            private long writtenSize;

            private void reinitWriter() throws IOException {
                ResourceId outputResource = FileSystems.matchNewResource(outputDir, true);
                ResourceId outputFileResource = outputResource.resolve("part-" + UUID.randomUUID() + ext, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
                this.writer = Channels.newWriter(FileSystems.create(outputFileResource, "application/octet-stream"), "UTF-8");
                this.writtenSize = 0;
                wroteHeader = false;
            }

            @Setup
            public void init() throws IOException {
                reinitWriter();
            }

            @ProcessElement
            public void processElement(@Element Row input, OutputReceiver<Void> output) throws IOException {
                if (!wroteHeader && headerEncoding != null) {
                    writeRecord(headerEncoding.toText(input, fields));
                    wroteHeader = true;
                }
                String toWrite = contentEncoding.toText(input, fields);
                writeRecord(toWrite);
                writtenSize += toWrite.getBytes(StandardCharsets.UTF_8).length;
                if (writtenSize >= 256_000_000) { // 256 mb max size
                    teardown();
                    reinitWriter();
                }
            }

            private void writeRecord(String text) {
                try {
                    writer.write(text + "\r\n");
                } catch (Throwable t) {
                    throw new RuntimeException(t);
                }
            }

            @Teardown
            public void teardown() {
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }));
        return PDone.in(input.getPipeline());
    }
}
