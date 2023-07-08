package org.ohnlp.backbone.api.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.schemas.Schema;
import org.ohnlp.backbone.api.*;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.components.ExtractComponent;
import org.ohnlp.backbone.api.components.HasInputs;
import org.ohnlp.backbone.api.components.legacy.v2.UsesLegacyConfigInit;
import org.ohnlp.backbone.api.components.legacy.v2.WrappedExtract;
import org.ohnlp.backbone.api.components.legacy.v2.WrappedLoad;
import org.ohnlp.backbone.api.components.legacy.v2.WrappedTransform;
import org.ohnlp.backbone.api.components.xlang.python.PythonProxyTransformComponent;
import org.ohnlp.backbone.api.config.xlang.JavaBackbonePipelineComponentConfiguration;
import org.ohnlp.backbone.api.config.xlang.PythonBackbonePipelineComponentConfiguration;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.api.config.BackboneConfiguration;
import org.ohnlp.backbone.api.config.BackbonePipelineComponentConfiguration;
import org.ohnlp.backbone.api.util.SchemaConfigUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.*;
import java.util.logging.Logger;

/**
 * This class generates an execution plan from a specified configuration instance
 */
public class PipelineBuilder {

    private static final Logger LOGGER = Logger.getLogger(PipelineBuilder.class.getName());

    /**
     * Builds an executable pipeline from a specified {@link BackboneConfiguration}
     *
     * @param config The configuration to use
     * @return The pipeline constructed from the supplied configuration
     * @throws ComponentInitializationException If an issue occurs loading transforms from configuration
     */
    @SuppressWarnings("rawtypes")
    public static ExecutionDAG getPipelineGraph(BackboneConfiguration config) throws ComponentInitializationException {
        BackbonePipelineComponentConfiguration[] configs = config.getPipeline().toArray(new BackbonePipelineComponentConfiguration[0]);
        if (configs.length < 2) {
            throw new ComponentInitializationException(new IllegalArgumentException("Pipelines must contain at a minimum a Extract and a Load"));
        }
        // Initialize and populate mapping of component IDs to instances, as well as a list of actual extracts (root nodes)
        List<String> extracts = new ArrayList<>();
        Map<String, InitializedPipelineComponent> componentsByID = new HashMap<>();
        for (int i = 0; i < configs.length; i++) {
            try {
                if (configs[i].getComponentID() == null) {
                    configs[i].setComponentID(i + "");
                }
                ComponentLang componentLang = configs[i].getLang();
                if (configs[i] instanceof JavaBackbonePipelineComponentConfiguration) {
                    // Handle Java Initialization
                    if (((configs[i].getInputs() == null || configs[i].getInputs().isEmpty()) && i > 0) && HasInputs.class.isAssignableFrom(((JavaBackbonePipelineComponentConfiguration)configs[i]).getClazz())) {
                        LOGGER.warning("A legacy (pre Backbone v3.0) pipeline configuration is being " +
                                "used with a Backbone v3.0+ installation. Input/Output associations between " +
                                "different components are being inferred. Running the configuration update script " +
                                "is strongly recommended for stability and to verify result correctness");
                        BackbonePipelineComponentConfiguration.InputDefinition generatedDef
                                = new BackbonePipelineComponentConfiguration.InputDefinition();
                        generatedDef.setComponentID((i - 1) + "");
                        generatedDef.setInputTag("*");
                        configs[i].setInputs(Collections.singletonMap("*", generatedDef));
                    }
                    Class<? extends BackbonePipelineComponent> clazz = ((JavaBackbonePipelineComponentConfiguration)configs[i]).getClazz();
                    Constructor<? extends BackbonePipelineComponent> ctor = clazz.getDeclaredConstructor();
                    BackbonePipelineComponent instance = ctor.newInstance();
                    JsonNode configForInstance = configs[i].getConfig();
                    if (instance instanceof UsesLegacyConfigInit) {
                        ((UsesLegacyConfigInit) instance).initFromConfig(configs[i].getConfig());
                        if (instance instanceof Extract) {
                            instance = new WrappedExtract((Extract) instance);
                        } else if (instance instanceof Transform) {
                            instance = new WrappedTransform((Transform) instance);
                        } else {
                            instance = new WrappedLoad((Load) instance);
                        }
                    } else {
                        injectInstanceWithConfigurationProperties(clazz, instance, configForInstance);
                        instance.init();
                    }
                    componentsByID.put(configs[i].getComponentID(), new InitializedPipelineComponent(configs[i].getComponentID(), configs[i].getInputs(), instance));
                    if (instance instanceof ExtractComponent) {
                        extracts.add(configs[i].getComponentID());
                    }
                } else if (configs[i] instanceof PythonBackbonePipelineComponentConfiguration) {
                    PythonBackbonePipelineComponentConfiguration pythonconfig = (PythonBackbonePipelineComponentConfiguration) configs[i];
                    PythonProxyTransformComponent instance = new PythonProxyTransformComponent(pythonconfig.getBundleName(), pythonconfig.getEntryPoint(), pythonconfig.getEntryClass());
                    JsonNode configForInstance = pythonconfig.getConfig();
                    instance.injectConfig(configForInstance);
                    instance.init();
                    componentsByID.put(configs[i].getComponentID(), new InitializedPipelineComponent(configs[i].getComponentID(), configs[i].getInputs(), instance));
                }
            } catch (Throwable t) {
                throw new ComponentInitializationException(t);
            }
        }
        // Now add information on what each component outputs to by crossreference
        componentsByID.forEach((id, component) -> {
            component.inputs.forEach((componentTag, inputDef) -> {
                if (!componentsByID.containsKey(inputDef.getComponentID())) {
                    throw new IllegalArgumentException("Component " + id + " declares input from " + inputDef.getComponentID() + " that does not exist");
                }
                componentsByID.get(inputDef.getComponentID()).outputs.add(id);
            });
        });
        // Now construct the actual DAG
        return new ExecutionDAG(extracts, componentsByID);
    }

    private static void injectInstanceWithConfigurationProperties(
            Class<? extends BackbonePipelineComponent> clazz,
            BackbonePipelineComponent instance,
            JsonNode configForInstance) throws IllegalAccessException {
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
                        } else {
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
                try {
                    if (f.getType().equals(Schema.class)) {
                        val = SchemaConfigUtils.jsonToSchema(curr);
                    } else {
                        // Use jackson to infer the value
                        val = om.treeToValue(curr, om.constructType(f.getGenericType()));
                    }
                } catch (Throwable t) {
                    throw new IllegalArgumentException("Failed to instantiate config object " + f.getName()
                            + " for component " + clazz.getName(), t);
                }
                // and set the field
                f.set(instance, val);
            }
        }
    }

    public static class InitializedPipelineComponent {
        String componentID;
        Map<String, BackbonePipelineComponentConfiguration.InputDefinition> inputs;
        List<String> outputs = new ArrayList<>();
        BackbonePipelineComponent component;

        public InitializedPipelineComponent(
                String componentID,
                Map<String, BackbonePipelineComponentConfiguration.InputDefinition> inputs,
                BackbonePipelineComponent component) {
            this.componentID = componentID;
            this.inputs = inputs == null ? new HashMap<>() : inputs;
            this.component = component;
        }

        public String getComponentID() {
            return componentID;
        }

        public void setComponentID(String componentID) {
            this.componentID = componentID;
        }

        public Map<String, BackbonePipelineComponentConfiguration.InputDefinition> getInputs() {
            return inputs;
        }

        public void setInputs(Map<String, BackbonePipelineComponentConfiguration.InputDefinition> inputs) {
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
