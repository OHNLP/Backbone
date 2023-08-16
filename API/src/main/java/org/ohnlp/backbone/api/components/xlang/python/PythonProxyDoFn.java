package org.ohnlp.backbone.api.components.xlang.python;

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
    private final String doFnEntryClass;
    private final String driverInfo;
    private final boolean multiInput;
    private final boolean multiOutput;
    private final File tmpDir;

    private transient PythonBridge<? extends PythonProcessingPartitionBasedDoFn<?, ?>> python;
    private transient PythonProcessingPartitionBasedDoFn<?, ?> proxiedDoFn;

    public PythonProxyDoFn(File tmpDir, String bundleName,  String doFnEntryPoint, String doFnEntryClass, String infoFromDriver, boolean multiInput, boolean multiOutput) {
        this.tmpDir = tmpDir;
        this.bundleName = bundleName;
        this.doFnEntryPoint = doFnEntryPoint;
        this.doFnEntryClass = doFnEntryClass;
        this.driverInfo = infoFromDriver;
        this.multiInput = multiInput;
        this.multiOutput = multiOutput;
    }


    @Setup
    public void init() throws IOException {
        Class<? extends PythonProcessingPartitionBasedDoFn<?, ?>> implCls = null;
        if (multiInput) {
            throw new UnsupportedOperationException("Multi-Input Python Transforms are currently not supported");
        } else {
            if (multiOutput) {
                implCls = PythonOneToManyTransformDoFn.class;
            } else {
                implCls = PythonOneToOneTransformDoFn.class;
            }
        }
        // Init python bridge
        this.python = new PythonBridge<>(this.tmpDir, this.bundleName, this.doFnEntryPoint, this.doFnEntryClass, implCls);
        this.python.startBridge();

        // Get proxied DoFn
        this.proxiedDoFn = this.python.getPythonEntryPoint();

        // Call initialization function with info from driver
        this.proxiedDoFn.init_from_driver(driverInfo);
    }

    @StartBundle
    public void startBundle() {
        // Execute onBundleStart for the proxied DoFn
        this.proxiedDoFn.on_bundle_start();
    }

    @ProcessElement
    public void process(ProcessContext pc) {
        // String => String since the requisite PTransform would have already handled conversion to/from JSON
        String input = pc.element();
        System.out.println("ProxyParDo: Processing " + input);
        PythonRow inputRow = this.proxiedDoFn.python_row_from_json_string(input);
        System.out.println("Sending converted row to apply: " + inputRow.toString());
        if (this.proxiedDoFn instanceof PythonOneToOneTransformDoFn) {
            ((PythonOneToOneTransformDoFn)this.proxiedDoFn).proxied_apply(inputRow).forEach(r ->
                    pc.output(this.proxiedDoFn.json_string_from_python_row(r)));
        } else if (this.proxiedDoFn instanceof PythonOneToManyTransformDoFn) {
            ((PythonOneToManyTransformDoFn)this.proxiedDoFn).proxied_apply(inputRow).forEach(r -> {
                System.out.println("Received from apply: " + r.get_row());
                pc.output(new TupleTag<>(r.get_tag()), this.proxiedDoFn.json_string_from_python_row(r.get_row()));
            });
        } else {
            throw new UnsupportedOperationException("Unsupported DoFn Class Type " + this.proxiedDoFn.getClass().getName()); // TODO other types
        }
    }

    @FinishBundle
    public void finishBundle() {
        try {
            this.proxiedDoFn.on_bundle_end();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @Teardown
    public void teardownPythonBridge() {
       this.python.shutdownBridge();
    }
}
