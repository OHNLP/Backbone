package org.ohnlp.backbone.io.local;

import com.google.protobuf.FieldType;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.ReadableDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.components.ExtractToMany;
import org.ohnlp.backbone.api.components.ExtractToOne;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.io.Repartition;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URI;
import java.sql.ResultSet;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@ComponentDescription(
        name = "Read CSVs from Filesystem",
        desc = "Reads CSVs from a Directory. If multiple files exist in the directory, " +
                "assumes they all follow the same schema and reads in parallel"
)
public class CSVExtract extends ExtractToMany {
    @ConfigurationProperty(
            path = "fileSystemPath",
            desc = "The file system path to read from"
    )
    private String dir;
    @ConfigurationProperty(
            path = "firstRowIsHeader",
            desc = "Whether the first row is a header column"
    )
    private boolean skipFirstRow;

    @ConfigurationProperty(
            path = "schema",
            desc = "CSV File Schema"
    )
    private Schema schema;

    private Schema errorSchema = Schema.of(
            Schema.Field.nullable("Record", Schema.FieldType.STRING),
            Schema.Field.nullable("Exception", Schema.FieldType.STRING)
    );

    @Override
    public void init() throws ComponentInitializationException {

    }

    @Override
    public PCollectionRowTuple expand(PBegin input) {
        PCollection<String> fileURIs = input.apply("Scan Input Directory for Partitioned Files", Create.of(Arrays.stream(Objects.requireNonNull(new File(dir).listFiles())).map(f -> f.toURI().toString()).collect(Collectors.toList()))).setCoder(StringUtf8Coder.of());
        PCollectionTuple readColls = fileURIs.apply("Read CSV Records and Map to Rows", ParDo.of(new DoFn<String, Row>() {
            private String[] header;
            private Map<Schema.TypeName, SerializableFunction<String, Object>> typeResolvers;

            @Setup
            public void init() {
                this.header = schema.getFields().stream().map(Schema.Field::getName).toArray(String[]::new);
                this.typeResolvers = new HashMap<>();
                this.typeResolvers.put(Schema.TypeName.BYTE, Byte::parseByte);
                this.typeResolvers.put(Schema.TypeName.BYTES, s -> {
                    throw new UnsupportedOperationException("Cannot deserialize byte arrays from CSV");
                });
                this.typeResolvers.put(Schema.TypeName.DATETIME, s -> ISODateTimeFormat.dateTimeParser().parseDateTime(s));
                this.typeResolvers.put(Schema.TypeName.DECIMAL, BigDecimal::new);
                this.typeResolvers.put(Schema.TypeName.DOUBLE, Double::parseDouble);
                this.typeResolvers.put(Schema.TypeName.FLOAT, Float::parseFloat);
                this.typeResolvers.put(Schema.TypeName.INT16, Short::parseShort);
                this.typeResolvers.put(Schema.TypeName.INT32, Integer::parseInt);
                this.typeResolvers.put(Schema.TypeName.INT64, Long::parseLong);
                this.typeResolvers.put(Schema.TypeName.STRING, s -> s);
            }

            @ProcessElement
            public void processFile(ProcessContext pc) {
                URI fileURI = URI.create(pc.element());
                try (Reader reader = new FileReader(new File(fileURI))) {
                    CSVFormat format = CSVFormat.EXCEL.withHeader(this.header).withSkipHeaderRecord(skipFirstRow).withNullString("");
                    CSVParser parser = format.parse(reader);
                    parser.iterator().forEachRemaining(r -> {
                        try {
                            Row.Builder rowBuilder = Row.withSchema(schema);
                            for (Schema.Field field : schema.getFields()) {
                                String textValue = r.get(field.getName());
                                if (textValue == null) {
                                    rowBuilder.addValue(null);
                                } else {
                                    SerializableFunction<String, Object> func = this.typeResolvers.get(field.getType().getTypeName());
                                    if (func == null) {
                                        throw new UnsupportedOperationException("Deserialization of Type " + field.getType().getTypeName().name() + " from CSV is not Supported");
                                    }
                                    rowBuilder.addValue(func.apply(textValue));
                                }
                            }
                            pc.output(new TupleTag<>("CSV Records"), rowBuilder.build());
                        } catch (Throwable t) {
                            pc.output(new TupleTag<>("Errored Records"), Row.withSchema(errorSchema).addValue(r.toString()).addValue(ExceptionUtils.getStackTrace(t)).build());
                        }
                    });
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }
        }).withOutputTags(new TupleTag<>("CSV Records"), TupleTagList.of(new TupleTag<>("Errored Records"))));
        PCollection<Row> read = readColls.get("CSV Records");
        read.setRowSchema(this.schema);
        PCollectionRowTuple ret = PCollectionRowTuple.of("CSV Records", read.apply("Break Fusion", Repartition.of())).and("Errored Records", readColls.get("Errored Records"));
        // Set Coders
        ret.get("CSV Records").setRowSchema(this.schema);
        ret.get("Errored Records").setRowSchema(this.errorSchema);
        return ret;
    }


    @Override
    public List<String> getOutputTags() {
        return Arrays.asList("CSV Records", "Errored Records");
    }

    @Override
    public Map<String, Schema> calculateOutputSchema(Map<String, Schema> input) {
        return Map.of(
                "CSV Records", this.schema,
                "Errored Records", this.errorSchema
        );
    }
}
