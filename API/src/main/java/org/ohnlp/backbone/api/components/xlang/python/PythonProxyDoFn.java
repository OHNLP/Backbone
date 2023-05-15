package org.ohnlp.backbone.api.components.xlang.python;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.io.*;

/**
 * A proxied <b>Row to Row</b> Python DoFn that implements {@link PythonProcessingPartitionBasedDoFn}
 *
 * Implementors should implement one of {@link PythonOneToOneTransformDoFn} or {@link PythonOneToManyTransformDoFn}
 * instead of directly instantiating this class.
 * <br/>
 * It is assumed that conversion of Rows to/from JSON String will have occurred prior to/subsequent to this step respectively.
 */
public class PythonProxyDoFn extends DoFn<String, String> implements Serializable {
    private final String bundleName;
    private final String doFnEntryPoint;
    private final String driverInfo;

    private transient PythonBridge<PythonProcessingPartitionBasedDoFn> python;
    private transient PythonProcessingPartitionBasedDoFn<?, ?> proxiedDoFn;

    public PythonProxyDoFn(String bundleName, String doFnEntryPoint, String infoFromDriver) {
        this.bundleName = bundleName;
        this.doFnEntryPoint = doFnEntryPoint;
        this.driverInfo = infoFromDriver;
    }


    @Setup
    public void init() throws IOException {
        // Init python bridge
        this.python = new PythonBridge<>(this.bundleName, this.doFnEntryPoint, PythonProcessingPartitionBasedDoFn.class);
        this.python.startBridge();

        // Get proxied DoFn
        this.proxiedDoFn = this.python.getPythonEntryPoint();

        // Call initialization function with info from driver
        this.proxiedDoFn.initFromDriver(driverInfo);
    }

    @StartBundle
    public void startBundle() {
        // Execute onBundleStart for the proxied DoFn
        this.proxiedDoFn.onBundleStart();
    }

    @ProcessElement
    public void process(ProcessContext pc) {
        // String => String since the requisite PTransform would have already handled conversion to/from JSON
        String input = pc.element();
        if (this.proxiedDoFn instanceof PythonOneToOneTransformDoFn) {
            ((PythonOneToOneTransformDoFn)this.proxiedDoFn).apply(input).forEach(pc::output);
        } else if (this.proxiedDoFn instanceof PythonOneToManyTransformDoFn) {
            ((PythonOneToManyTransformDoFn)this.proxiedDoFn).apply(input).forEach(r -> pc.output(new TupleTag<>(r.getTag()), r.getRow()));
        } // TODO other types
    }

    @FinishBundle
    public void finishBundle() {
        try {
            this.proxiedDoFn.onBundleEnd();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @Teardown
    public void teardownPythonBridge() {
       this.python.shutdownBridge();
    }
}
