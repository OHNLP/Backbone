package org.ohnlp.backbone.api.components.wrappers.lang.python;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionRowTuple;

import java.io.*;
import java.nio.file.Files;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class PythonDoFn extends DoFn<PCollectionRowTuple, PCollectionRowTuple> {
    private final String bundleName;
    private final String entryPoint;
    private File envDir;

    public PythonDoFn(String bundleName, String entryPoint) {
        this.bundleName = bundleName;
        this.entryPoint = entryPoint;
    }

    @Setup
    public void setupPythonBridge() {
        // First, extract resources into a temp folder. This should have bin/python as an entry point
        extractPythonResources();
    }

    @Teardown
    public void teardownPythonBridge() {
        deleteRecurs(this.envDir);
    }

    public void deleteRecurs(File parent) {
        File[] children = parent.listFiles();
        if (children != null) {
            for (File file : children) {
                deleteRecurs(file);
            }
        }
        parent.delete();
    }


    /*
     * Because Backbone bundles everything into a master jar file as part of its build process, we need to extract this into
     * a tmp folder to properly execute the python process with associated dependencies
     */
    public void extractPythonResources() {
        String name = this.getClass().getSimpleName() + "_tmp_" + UUID.randomUUID();
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        this.envDir = new File(tmpDir, name);
        this.envDir.mkdirs();
        try (ZipInputStream zis = new ZipInputStream(getClass().getResourceAsStream("/python_modules/" + this.bundleName))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                String pathRelative = entry.getName();
                File pathInTmp = new File(envDir, pathRelative);
                byte[] contents = zis.readAllBytes();
                try (FileOutputStream fos = new FileOutputStream(pathInTmp)) {
                    fos.write(contents);
                    fos.flush();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
