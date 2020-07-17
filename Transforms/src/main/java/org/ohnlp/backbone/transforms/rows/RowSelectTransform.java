package org.ohnlp.backbone.transforms.rows;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Transform;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.util.LinkedList;
import java.util.List;

/**
 * Transform that performs selection (filtering) of columns within a {@link Row} as well as the option to rename
 * columns wrapped as a Backbone configuration-compatible element
 *
 * <p>
 * Expected configuration structure <b>Note: </b> config structure is an array:
 * <pre>
 *     [
 *         "inputFieldName1",
 *         "inputFieldName2|outputFieldName2",
 *         ...
 *         etc.
 *     ]
 * </pre>
 */
public class RowSelectTransform extends Transform {

    private Select.Fields<Row> select;

    @Override
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        List<String> fields = new LinkedList<>();
        for (JsonNode node : config) {
            fields.add(node.asText());
        }
        this.select
                = Select.fieldNames(fields.stream().map(s -> s.contains("|") ? s.substring(0, s.indexOf('|')) : s).toArray(String[]::new));
        fields.forEach(mapping -> {
            if (mapping.contains("|")) {
                String[] map = mapping.split("\\|");
                select = select.withFieldNameAs(map[0], map[1]);
            }
        });
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        return input.apply(select);
    }
}
