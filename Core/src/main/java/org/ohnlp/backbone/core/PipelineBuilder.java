package org.ohnlp.backbone.core;

import org.apache.beam.sdk.transforms.PTransform;
import org.ohnlp.backbone.api.BackbonePipelineComponent;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.Load;
import org.ohnlp.backbone.api.Transform;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.core.config.BackboneConfiguration;
import org.ohnlp.backbone.core.config.BackbonePipelineComponentConfiguration;

import java.lang.reflect.Constructor;
import java.util.LinkedList;
import java.util.List;

/**
 * This class generates an execution plan from a specified configuration instance
 */
public class PipelineBuilder {
    /**
     * Builds a executable pipeline from a specified {@link BackboneConfiguration}
     *
     * @param config The configuration to use
     * @return A sequence of transforms constructed from the supplied configuration
     * @throws ComponentInitializationException If an issue occurs loading transforms from configuration
     */
    @SuppressWarnings("rawtypes")
    public static List<BackbonePipelineComponent<?, ?>> buildTransformsFromConfig(BackboneConfiguration config) throws ComponentInitializationException {
        LinkedList<BackbonePipelineComponent<?, ?>> pipelineConfig = new LinkedList<>();
        BackbonePipelineComponentConfiguration[] configs = config.getPipeline().toArray(new BackbonePipelineComponentConfiguration[0]);
        for (int i = 0; i < configs.length; i++) {
            try {
                Class<? extends BackbonePipelineComponent> clazz = configs[i].getClazz();
                Constructor<? extends BackbonePipelineComponent> ctor = clazz.getDeclaredConstructor();
                BackbonePipelineComponent instance = ctor.newInstance();
                instance.initFromConfig(configs[i].getConfig());
                pipelineConfig.addLast(instance);
                if (i == 0 && !(instance instanceof Extract)) {
                    throw new IllegalArgumentException("Pipelines must begin with an extract operation, " +
                            "found a " + instance.getClass().getName() + " instead!");
                } if (i == configs.length - 1  && !(instance instanceof Load)) {
                    throw new IllegalArgumentException("Pipelines must end with a Load operation, " +
                            "found a " + instance.getClass().getName() + " instead!");
                } else if (!(instance instanceof Transform)) {
                    throw new IllegalArgumentException("Intermediate pipeline operations must be transforms, found a " +
                            instance.getClass().getName() + " instead at index " + i);
                }
            } catch (Throwable t) {
                throw new ComponentInitializationException(t);
            }
        }
        return pipelineConfig;
    }

}
