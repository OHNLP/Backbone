package org.ohnlp.backbone.api.components.xlang.python;

import java.util.List;

public interface PythonProcessingPartitionBasedDoFn<I, O> {

    /**
     * @param configFromDriver A JSON Config originating post-initialization from the driver
     */
    void initFromDriver(String configFromDriver);

    /**
     * Defines code to execute when a bundle/partition is started.
     *
     */
    void onBundleStart();

    /**
     * Defines code to execute when a bundle is finished processing
     */
    void onBundleEnd();

    /**
     * @param input a single input record
     * @return A list of output records
     */
    List<O> apply(I input);
}
