package org.ohnlp.backbone.transforms.multiinput;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.components.TransformComponent;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.util.*;

@ComponentDescription(
        name = "Union Inputs Into Single Output",
        desc = "Merges multiple inputs together into a single output"
)
public class Union extends TransformComponent {

    @ConfigurationProperty(
            path = "distinct",
            desc = "Whether to remove any duplicate rows from the output"
    )
    private boolean distinct;

    @Override
    public void init() throws ComponentInitializationException {

    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
        PCollection<Row> out = PCollectionList.of(input.getAll().values()).apply("Union", Flatten.pCollections());
        if (distinct) {
            out = out.apply("Distinct", Distinct.create());
        }
        return PCollectionRowTuple.of("Union Output", out);
    }

    @Override
    public List<String> getInputTags() {
        return Arrays.asList("1", "2");
    }

    @Override
    public List<String> getOutputTags() {
        return Collections.singletonList("Union Output");
    }

    @Override
    public Map<String, Schema> calculateOutputSchema(Map<String, Schema> input) {
        return Map.of("Union Output", input.values().stream().filter(Objects::nonNull).findFirst().get());
    }
}
