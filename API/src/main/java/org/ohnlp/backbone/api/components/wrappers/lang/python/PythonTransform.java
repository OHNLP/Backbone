package org.ohnlp.backbone.api.components.wrappers.lang.python;

import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.ohnlp.backbone.api.components.TransformComponent;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import py4j.ClientServer;

import java.io.File;
import java.util.UUID;

public abstract class PythonTransform extends TransformComponent {

    protected ClientServer pythonClient;
    private File envDir;

    @Override
    public void init() throws ComponentInitializationException {
        // Initialize the python package
        this.pythonClient = new ClientServer.ClientServerBuilder().pythonPort(0).javaPort(0).build();
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {

        return null;
    }

}
