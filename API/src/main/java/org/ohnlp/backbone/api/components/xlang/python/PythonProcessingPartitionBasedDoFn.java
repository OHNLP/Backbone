package org.ohnlp.backbone.api.components.xlang.python;

import java.util.List;

public interface PythonProcessingPartitionBasedDoFn<I, O> extends PythonStructureProxy {

    /**
     * @param configFromDriver A JSON Config originating post-initialization from the driver
     */
    void init_from_driver(String configFromDriver);

    /**
     * Defines code to execute when a bundle/partition is started.
     *
     */
    void on_bundle_start();

    /**
     * Defines code to execute when a bundle is finished processing
     */
    void on_bundle_end();

    /**
     * @param input a single input record
     * @return A list of output records
     */
    List<O> proxied_apply(I input);
}
