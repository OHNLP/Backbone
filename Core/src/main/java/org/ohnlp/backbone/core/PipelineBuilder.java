package org.ohnlp.backbone.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.schemas.Schema;
import org.ohnlp.backbone.api.BackbonePipelineComponent;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.Load;
import org.ohnlp.backbone.api.Transform;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.core.config.BackboneConfiguration;
import org.ohnlp.backbone.core.config.BackbonePipelineComponentConfiguration;
import org.ohnlp.backbone.core.pipeline.ExecutionDAG;
import org.ohnlp.backbone.io.util.ConfigUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.*;

/**
 * This class generates an execution plan from a specified configuration instance
 */
public class PipelineBuilder {
    /**
     * Builds an executable pipeline from a specified {@link BackboneConfiguration}
     *
     * @param config The configuration to use
     * @return The pipeline constructed from the supplied configuration
     * @throws ComponentInitializationException If an issue occurs loading transforms from configuration
     */
    @SuppressWarnings("rawtypes")
    public static ExecutionDAG  getPipelineGraph(BackboneConfiguration config) throws ComponentInitializationException {
        BackbonePipelineComponentConfiguration[] configs = config.getPipeline().toArray(new BackbonePipelineComponentConfiguration[0]);
        if (configs.length < 2) {
            throw new ComponentInitializationException(new IllegalArgumentException("Pipelines must contain at a minimum a Extract and a Load"));
        }
        // Initialize and populate mapping of component IDs to instances, as well as a list of actual extracts (root nodes)
        List<String> extracts = new ArrayList<>();
        Map<String, InitializedPipelineComponent> componentsByID = new HashMap<>();
        for (int i = 0; i < configs.length; i++) {
            try {
                if (configs[i].getStepId() == null) {
                    configs[i].setStepId(i + "");
                }
                if ((configs[i].getInputIds() == null || configs[i].getInputIds().isEmpty()) && i > 0) {
                    configs[i].setInputIds(Collections.singletonList((i-1) + ""));
                }
                Class<? extends BackbonePipelineComponent> clazz = configs[i].getClazz();
                Constructor<? extends BackbonePipelineComponent> ctor = clazz.getDeclaredConstructor();
                BackbonePipelineComponent instance = ctor.newInstance();
                JsonNode configForInstance = configs[i].getConfig();
                injectInstanceWithConfigurationProperties(clazz, instance, configForInstance);
                instance.init();
                componentsByID.put(configs[i].getStepId(), new InitializedPipelineComponent(configs[i].getStepId(), configs[i].getInputIds(), instance));
                if (instance instanceof Extract) {
                    extracts.add(configs[i].getStepId());
                }
            } catch (Throwable t) {
                throw new ComponentInitializationException(t);
            }
        }
        // Now add information on what each component outputs to by crossreference
        componentsByID.forEach((id, component) -> {
            for (String input : component.inputs) {
                if (!componentsByID.containsKey(input)) {
                    throw new IllegalArgumentException("Component " + id + " declares input from " + input + " that does not exist");
                }
                componentsByID.get(input).outputs.add(id);
            }
        });
        // Now construct the actual DAG
        return new ExecutionDAG(extracts, componentsByID);
    }

    private static void injectInstanceWithConfigurationProperties(
            Class<? extends BackbonePipelineComponent> clazz,
            BackbonePipelineComponent instance,
            JsonNode configForInstance) throws JsonProcessingException, IllegalAccessException {
        ObjectMapper om = new ObjectMapper();
        for (Field f : clazz.getDeclaredFields()) {
            if (f.isAnnotationPresent(ConfigurationProperty.class)) {
                ConfigurationProperty configSettings = f.getDeclaredAnnotation(ConfigurationProperty.class);
                JsonNode curr = configForInstance;
                String[] path = configSettings.path().split("\\.");
                for (int i = 0; i < path.length; i++) {
                    if (curr.has(path[i])) {
                        curr = curr.get(path[i]);
                    } else {
                        if (configSettings.required()) {
                            throw new IllegalArgumentException("Config setting " + configSettings.path() + " is required but not provided for " + clazz.getName() + " config");
                        }
                        else {
                            curr = null;
                            break;
                        }
                    }
                }
                if (curr == null) {
                    continue; // Non-required (or exception thrown) and no settings provided, move on to next
                }
                f.setAccessible(true);
                Object val;
                if (f.getType().equals(Schema.class)) {
                    val = ConfigUtils.resolveObjectSchema(curr);
                } else {
                    // Use jackson to infer the value
                    val = om.treeToValue(curr, om.constructType(f.getGenericType()));
                }
                // and set the field
                f.set(instance, val);
            }
        }
    }

    public static class InitializedPipelineComponent {
        String componentID;
        List<String> inputs;
        List<String> outputs = new ArrayList<>();
        BackbonePipelineComponent component;

        public InitializedPipelineComponent(String componentID, List<String> inputs, BackbonePipelineComponent component) {
            this.componentID = componentID;
            this.inputs = inputs;
            this.component = component;
        }

        public String getComponentID() {
            return componentID;
        }

        public void setComponentID(String componentID) {
            this.componentID = componentID;
        }

        public List<String> getInputs() {
            return inputs;
        }

        public void setInputs(List<String> inputs) {
            this.inputs = inputs;
        }

        public List<String> getOutputs() {
            return outputs;
        }

        public void setOutputs(List<String> outputs) {
            this.outputs = outputs;
        }

        public BackbonePipelineComponent getComponent() {
            return component;
        }

        public void setComponent(BackbonePipelineComponent component) {
            this.component = component;
        }
    }


}
