package org.ohnlp.backbone.core.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.POutput;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.Load;
import org.ohnlp.backbone.api.components.*;
import org.ohnlp.backbone.core.PipelineBuilder;
import org.ohnlp.backbone.core.config.BackbonePipelineComponentConfiguration;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
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
            ExtractComponent extract = (ExtractComponent) componentMeta.getComponent();
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
            for (Map.Entry<String, BackbonePipelineComponentConfiguration.InputDefinition> e : component.getInputs().entrySet()) {
                if (!outputCollsByID.containsKey(e.getValue().getComponentID())) {
                    processNow = false;
                    break;
                }
            }
            if (!processNow) {
                deferred.add(component);
                continue;
            }
            // First, map input to output
            AtomicReference<PCollectionRowTuple> inputToNextStep = new AtomicReference<>(PCollectionRowTuple.empty(p));
            component.getInputs().forEach((outputTag, inputDef) -> {
                PCollectionRowTuple prev = outputCollsByID.get(inputDef.getComponentID());
                if (component.getComponent() instanceof SingleInputComponent) {
                    if (prev.getAll().size() != 1) {
                        if (inputDef.getInputTag().equals("*")) {
                            throw new IllegalArgumentException(component.getComponentID() + " expects a single input but "
                                    + prev.getAll().size() + " inputs are created in previous step. An explicit mapping must be provided!");
                        }
                        else {
                            if (!prev.has(inputDef.getInputTag())) {
                                throw new IllegalArgumentException(component.getComponentID()
                                        + " requested input " + inputDef.getInputTag()
                                        + " which does not exist amongst the outputs for the previous step. " +
                                        "Available inputs: " + String.join(",", prev.getAll().keySet()));
                            }
                            inputToNextStep.set(PCollectionRowTuple.of(outputTag, prev.get(inputDef.getInputTag())));
                        }
                    } else {
                        inputToNextStep.set(PCollectionRowTuple.of(outputTag, prev.get(prev.getAll().keySet().toArray(new String[0])[0])));
                    }
                } else {
                    String inputTag = inputDef.getInputTag();
                    if (!prev.has(inputTag)) {
                        throw new IllegalArgumentException(component.getComponentID()
                                + " requested input " + inputDef.getInputTag() + " for output " + outputTag
                                + " which does not exist amongst the outputs for the previous step. " +
                                "Available inputs: " + String.join(",", prev.getAll().keySet()));
                    }
                    inputToNextStep.set(inputToNextStep.get().and(outputTag, prev.get(inputTag)));
                }
            });
            if (component.getComponent() instanceof TransformComponent) {
                PCollectionRowTuple df = inputToNextStep.get().apply(component.getComponentID(), (TransformComponent)component.getComponent());
                outputCollsByID.put(component.getComponentID(), df);
            } else if (component.getComponent() instanceof LoadComponent) {
                inputToNextStep.get().apply(component.getComponentID(), (LoadComponent)component.getComponent());
            } else {
                throw new IllegalArgumentException("A extract step is provided with inputs!");
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
            if ((cmp.getOutputs() == null || cmp.getOutputs().size() == 0) && !(cmp.getComponent() instanceof LoadComponent)) {
                Logger.getLogger("org.ohnlp.backbone.BackboneRunner")
                        .warning("An orphan transform step " + id +
                                " exists with no output. If this is not expected, " +
                                "(e.g. output is being written as part of a transform passthrough), " +
                                "remove this component chain as extraneous computation is being performed");
            }
            if (cmp.getInputs() != null && cmp.getInputs().size() != 1 && cmp.getComponent() instanceof SingleInputComponent) {
                throw new IllegalArgumentException("A simple Transform step expects 1 input source, but " + cmp.getInputs().size() + " was specified for Transform ID " + cmp.getComponentID());
            }
        });
        // TODO check graph acyclic
        // TODO check input/output column names
    }
}
