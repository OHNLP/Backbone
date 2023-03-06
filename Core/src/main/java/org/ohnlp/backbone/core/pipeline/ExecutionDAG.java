package org.ohnlp.backbone.core.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.Load;
import org.ohnlp.backbone.api.transforms.OneToOneTransform;
import org.ohnlp.backbone.core.PipelineBuilder;

import java.util.*;
import java.util.logging.Logger;

public class ExecutionDAG {

    private final List<String> extracts;
    private final Map<String, PipelineBuilder.InitializedPipelineComponent> componentsByID;

    public ExecutionDAG(List<String> extracts, Map<String, PipelineBuilder.InitializedPipelineComponent> componentsByID) {
        this.extracts = extracts;
        this.componentsByID = componentsByID;
    }

    public void planDAG(Pipeline pipeline) {
        checkPipelineValidity();
        Map<String, PCollectionRowTuple> outputCollsByID = new HashMap<>();
        Set<PipelineBuilder.InitializedPipelineComponent> componentsToProcess = new HashSet<>();
        for (String extractID : extracts) {
            PBegin start = PBegin.in(pipeline);
            PipelineBuilder.InitializedPipelineComponent componentMeta = componentsByID.get(extractID);
            Extract extract = (Extract) componentMeta.getComponent();
            Map<String, Schema> out = extract.calculateOutputSchema(null);
            PCollectionRowTuple df = start.apply("Step-" + extractID, extract);
            for (String tag : out.keySet()) {
                if (!df.get(tag).hasSchema()) {
                    df.get(tag).setRowSchema(out.get(tag));
                }
            }
            for (String next : componentMeta.getOutputs()) {
                componentsToProcess.add(componentsByID.get(next));
            }
            outputCollsByID.put(extractID, df);
        }
        while (componentsToProcess.size() > 0) {
            componentsToProcess = runNextBatchOrDefer(pipeline, componentsToProcess, outputCollsByID);
        }
        outputCollsByID.clear();
    }

    private Set<PipelineBuilder.InitializedPipelineComponent> runNextBatchOrDefer(
            Pipeline p,
            Set<PipelineBuilder.InitializedPipelineComponent> componentsToProcess,
            Map<String, PCollectionRowTuple> outputCollsByID) {
        Set<PipelineBuilder.InitializedPipelineComponent> deferred = new HashSet<>();
        for (PipelineBuilder.InitializedPipelineComponent component : componentsToProcess) {
            boolean processNow = true;
            for (String requirement : component.getInputs()) {
                if (!outputCollsByID.containsKey(requirement)) {
                    processNow = false;
                    break;
                }
            }
            if (!processNow) {
                deferred.add(component);
                continue;
            }
            if (component.getComponent() instanceof OneToOneTransform) {
                PCollectionRowTuple df =
                        outputCollsByID.get(component.getInputs().get(0))
                                .apply("Step-" + component.getComponentID(), (OneToOneTransform)component.getComponent());
                outputCollsByID.put(component.getComponentID(), df);
                for (String next : component.getOutputs()) {
                    deferred.add(componentsByID.get(next));
                }
            } else {
                // TODO needs to be implemented
            }
        }
        return deferred;
    }

    private void checkPipelineValidity() {
        for (String id : extracts) {
            if (componentsByID.get(id).getInputs() != null && componentsByID.get(id).getInputs().size() > 0) {
                throw new IllegalArgumentException("An extract step " + id + "is listed with required inputs");
            }
        }
        componentsByID.forEach((id, cmp) -> {
            if ((cmp.getOutputs() == null || cmp.getOutputs().size() == 0) && !(cmp.getComponent() instanceof Load)) {
                Logger.getLogger("org.ohnlp.backbone.BackboneRunner")
                        .warning("An orphan transform step " + id +
                                " exists with no output. If this is not expected, " +
                                "(e.g. output is being written as part of a transform passthrough), " +
                                "remove this component chain as extraneous computation is being performed");
            }
            if (cmp.getInputs() != null && cmp.getInputs().size() != 1 && cmp.getComponent() instanceof OneToOneTransform) {
                throw new IllegalArgumentException("A simple Transform step expects 1 input source, but " + cmp.getInputs().size() + " was specified for Transform ID " + cmp.getComponentID());
            }
        });
        // TODO check graph acyclic
        // TODO check input/output column names
    }
}
