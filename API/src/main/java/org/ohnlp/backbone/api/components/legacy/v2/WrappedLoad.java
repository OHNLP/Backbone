package org.ohnlp.backbone.api.components.legacy.v2;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Load;
import org.ohnlp.backbone.api.components.LoadFromOne;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

public class WrappedLoad extends LoadFromOne {
    private final Load component;

    public WrappedLoad(Load component) {
        this.component = component;
    }

    @Override
    public void init() throws ComponentInitializationException {

    }

    @Override
    public POutput expand(PCollection<Row> input) {
        return input.apply(component);
    }
}
