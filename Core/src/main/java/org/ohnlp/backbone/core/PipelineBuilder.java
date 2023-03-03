package org.ohnlp.backbone.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.transforms.PTransform;
import org.ohnlp.backbone.api.BackbonePipelineComponent;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.Load;
import org.ohnlp.backbone.api.Transform;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.core.config.BackboneConfiguration;
import org.ohnlp.backbone.core.config.BackbonePipelineComponentConfiguration;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

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
    public static BackboneETLPipeline buildETLPipelineFromConfig(BackboneConfiguration config) throws ComponentInitializationException {
        BackboneETLPipeline pipeline = new BackboneETLPipeline();
        LinkedList<Transform> transforms = new LinkedList<>();
        BackbonePipelineComponentConfiguration[] configs = config.getPipeline().toArray(new BackbonePipelineComponentConfiguration[0]);
        if (configs.length < 2) {
            throw new ComponentInitializationException(new IllegalArgumentException("Pipelines must contain at a minimum a Extract and a Load"));
        }
        for (int i = 0; i < configs.length; i++) {
            try {
                Class<? extends BackbonePipelineComponent> clazz = configs[i].getClazz();
                Constructor<? extends BackbonePipelineComponent> ctor = clazz.getDeclaredConstructor();
                BackbonePipelineComponent instance = ctor.newInstance();
                JsonNode configForInstance = configs[i].getConfig();
                if (i == 0) {
                    if (!(instance instanceof Extract)) {
                        throw new IllegalArgumentException("Pipelines must begin with an extract operation, " +
                                "found a " + instance.getClass().getName() + " instead!");
                    } else {
                        injectInstanceWithConfigurationProperties(clazz, instance, configForInstance);
                        instance.init();
                        pipeline.setExtract((Extract) instance);
                    }
                } else if (i == configs.length - 1) {
                    if (!(instance instanceof Load)) {
                        throw new IllegalArgumentException("Pipelines must end with a Load operation, " +
                                "found a " + instance.getClass().getName() + " instead!");
                    } else {
                        injectInstanceWithConfigurationProperties(clazz, instance, configForInstance);
                        instance.init();
                        pipeline.setLoad((Load) instance);
                    }
                } else {
                    if (!(instance instanceof Transform)) {
                        throw new IllegalArgumentException("Intermediate pipeline operations must be transforms, found a " +
                                instance.getClass().getName() + " instead at index " + i);
                    } else {
                        injectInstanceWithConfigurationProperties(clazz, instance, configForInstance);
                        instance.init();
                        transforms.addLast((Transform) instance);
                    }
                }

            } catch (Throwable t) {
                throw new ComponentInitializationException(t);
            }
        }
        pipeline.setTransforms(transforms);
        return pipeline;
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
                // Use jackson to infer the value
                Object val = om.treeToValue(curr, om.constructType(f.getGenericType()));
                // and set the field
                f.set(instance, val);
            }
        }
    }


    public static class BackboneETLPipeline {
        public Extract extract;
        public List<Transform> transforms;
        public Load load;

        public Extract getExtract() {
            return extract;
        }

        public void setExtract(Extract extract) {
            this.extract = extract;
        }

        public List<Transform> getTransforms() {
            return transforms;
        }

        public void setTransforms(List<Transform> transforms) {
            this.transforms = transforms;
        }

        public Load getLoad() {
            return load;
        }

        public void setLoad(Load load) {
            this.load = load;
        }
    }
}
