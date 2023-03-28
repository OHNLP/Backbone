package org.ohnlp.backbone.api.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.*;
import org.ohnlp.backbone.api.components.*;
import org.ohnlp.backbone.api.config.BackbonePipelineComponentConfiguration;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
            Map<String, Schema> out;
            if (!extract.isSchemaInit()) {
                out = extract.calculateOutputSchema(null);
                extract.setSchemaInit();
                extract.setComponentSchema(out);
            } else {
                out = extract.getComponentSchema();
            }
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
            Map<String, Schema> inputSchemas = new HashMap<>();
            component.getInputs().forEach((outputTag, inputDef) -> {
                PCollectionRowTuple prev = outputCollsByID.get(inputDef.getComponentID());
                if (component.getComponent() instanceof SingleInputComponent) {
                    if (prev.getAll().size() != 1) {
                        if (inputDef.getInputTag().equals("*")) {
                            throw new IllegalArgumentException(component.getComponentID() + " expects a single input but "
                                    + prev.getAll().size() + " inputs are created in previous step. An explicit mapping must be provided!");
                        } else {
                            if (!prev.has(inputDef.getInputTag())) {
                                throw new IllegalArgumentException(component.getComponentID()
                                        + " requested input " + inputDef.getInputTag()
                                        + " which does not exist amongst the outputs for the previous step. " +
                                        "Available inputs: " + String.join(",", prev.getAll().keySet()));
                            }
                            inputToNextStep.set(PCollectionRowTuple.of(outputTag, prev.get(inputDef.getInputTag())));
                            inputSchemas.put(outputTag, prev.get(inputDef.getInputTag()).getSchema());
                        }
                    } else {
                        inputToNextStep.set(PCollectionRowTuple.of(outputTag, prev.get(prev.getAll().keySet().toArray(new String[0])[0])));
                        inputSchemas.put(outputTag, prev.get(prev.getAll().keySet().toArray(new String[0])[0]).getSchema());
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
                    inputSchemas.put(outputTag, prev.get(inputTag).getSchema());
                }
            });
            // First initiate schema if necessary
            Map<String, Schema> nextSchema = new HashMap<>();
            boolean initSchema = false;
            if (component.getComponent() instanceof SchemaInitializable) {
                if (((SchemaInitializable) component.getComponent()).isSchemaInit()) {
                    nextSchema = ((SchemaInitializable) component.getComponent()).getComponentSchema();
                } else {
                    initSchema = true;
                }
            }
            // Check schema for required columns if any
            if (component.getComponent() instanceof HasInputs) {
                Map<String, PCollection<Row>> inputs = inputToNextStep.get().getAll();
                inputs.forEach((tag, coll) -> {
                    Schema s = ((HasInputs) component.getComponent()).getRequiredColumns(tag);
                    if (s != null) {
                        // Has required columns
                        Schema inputSchema = coll.getSchema();
                        List<String> requiredButMissing = new ArrayList<>();
                        for (Schema.Field f : s.getFields()) {
                            if (!inputSchema.hasField(f.getName())) {
                                // TODO do type checking
                                requiredButMissing.add(f.getName());
                            }
                        }
                        if (!requiredButMissing.isEmpty()) {
                            throw new IllegalArgumentException(component.getComponentID()
                                    + " requires the following fields that are missing from the supplied input: " +
                                    "[" + String.join(",", requiredButMissing) + "]. Available Fields: [" +
                                    String.join(",", inputSchema.getFieldNames()) + "]");
                        }

                    }
                });
            }
            if (component.getComponent() instanceof TransformComponent) {
                if (initSchema) {
                    nextSchema = ((TransformComponent) component.getComponent()).calculateOutputSchema(inputSchemas);
                    ((TransformComponent) component.getComponent()).setSchemaInit();
                    ((TransformComponent) component.getComponent()).setComponentSchema(nextSchema);
                }
                PCollectionRowTuple df = inputToNextStep.get().apply(component.getComponentID(), (TransformComponent) component.getComponent());
                outputCollsByID.put(component.getComponentID(), df);
                for (String tag : nextSchema.keySet()) {
                    if (!df.get(tag).hasSchema()) {
                        df.get(tag).setRowSchema(nextSchema.get(tag));
                    }
                }
            } else if (component.getComponent() instanceof LoadComponent) {
                inputToNextStep.get().apply(component.getComponentID(), (LoadComponent) component.getComponent());
            } else {
                throw new IllegalArgumentException("An extract step is provided with inputs!");
            }
            if (component.getOutputs() != null) {
                deferred.addAll(component.getOutputs().stream().map(componentsByID::get).collect(Collectors.toList()));
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
