package org.ohnlp.backbone.core.coders;

import org.apache.beam.sdk.schemas.Schema;

import java.util.LinkedList;
import java.util.List;

public final class NoteNLPSchema {

    public static Schema getSchema() {
        List<Schema.Field> fields = new LinkedList<>();
        fields.add(Schema.Field.of("note_id", Schema.FieldType.INT32));
        fields.add(Schema.Field.of("note_text", Schema.FieldType.STRING));
        fields.add(Schema.Field.of("nlp_output_json", Schema.FieldType.STRING));
        fields.add(Schema.Field.of("section_concept_id", Schema.FieldType.INT32));
        fields.add(Schema.Field.of("lexical_variant", Schema.FieldType.STRING));
        fields.add(Schema.Field.of("snippet", Schema.FieldType.STRING));
        fields.add(Schema.Field.of("note_nlp_concept_id", Schema.FieldType.INT32));
        fields.add(Schema.Field.of("note_nlp_source_concept_id", Schema.FieldType.INT32));
        fields.add(Schema.Field.of("nlp_datetime", Schema.FieldType.DATETIME));
        fields.add(Schema.Field.of("term_modifiers", Schema.FieldType.STRING));
        fields.add(Schema.Field.of("offset", Schema.FieldType.INT32));
        Schema schema = Schema.of(fields.toArray(new Schema.Field[0]));
        return schema;
    }

}
