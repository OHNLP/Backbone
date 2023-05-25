package org.ohnlp.backbone.api.components.xlang.python;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import py4j.ClientServer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.nio.file.*;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

public class PythonBridge<T> implements Serializable {
    private final long pythonInitTimeout = 30000; // TODO make this configurable
    private final String bundleName;
    private final String entryPoint;
    private final Class<T> pythonEntryPointClass;
    private transient File envDir;
    private transient DefaultExecutor executor;
    private transient ClientServer bridgeServer;

    public PythonBridge(String bundleName, String entryPoint, Class<T> clazz) throws IOException {
        this.bundleName = bundleName;
        this.entryPoint = entryPoint;
        this.pythonEntryPointClass = clazz;
    }

    public void startBridge() throws IOException {
        extractPythonResources();
        startServer();
    }

    public T getPythonEntryPoint() {
        return (T) this.bridgeServer.getPythonServerEntryPoint(new Class<>[] {this.pythonEntryPointClass});
    }

    /*
     * Because Backbone bundles everything into a master jar file as part of its build process, we need to extract this into
     * a tmp folder to properly execute the python process with associated dependencies
     */
    private void extractPythonResources() {
        String name = this.getClass().getSimpleName() + "_tmp_" + UUID.randomUUID();
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        this.envDir = new File(tmpDir, name);
        this.envDir.mkdirs();
        // Extract both the python launcher script and the bundle itself
        for (String bundle : Arrays.asList("backbone_python_launcher.zip", this.bundleName)) {
            try (ZipInputStream zis = new ZipInputStream(getClass().getResourceAsStream("/python_modules/" + bundle))) {
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    if (entry.isDirectory()) {
                        continue;
                    }
                    String pathRelative = entry.getName();
                    File pathInTmp = new File(this.envDir, pathRelative);
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

    /*
     * Spins up the python process/py4j bridge
     */
    private void startServer() throws IOException {
        // Create sentinel watcher for python process initialization
        WatchService watcher = FileSystems.getDefault().newWatchService();
        File done = new File(this.envDir, "python_bridge_meta.done");
        this.envDir.toPath().register(watcher, ENTRY_CREATE);
        // Launch the python process
        String cmd = String.join(" ", new File("bin", "python").getAbsolutePath(), "backbone_module_launcher.py", entryPoint);
        CommandLine cmdLine = CommandLine.parse(cmd);
        this.executor = new DefaultExecutor();
        this.executor.setWorkingDirectory(this.envDir);
        try {
            this.executor.execute(cmdLine, new ExecuteResultHandler() {
                @Override
                public void onProcessComplete(int exitValue) {
                    // TODO
                }

                @Override
                public void onProcessFailed(ExecuteException e) {
                    try {
                        shutdownBridge();
                    } catch (Throwable ignored) {}
                    throw new RuntimeException("Broken Python Bridge", e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Wait for python process to start and the python_bridge_meta.done file to be created
        long start = System.currentTimeMillis();
        boolean initDone = false;
        while (!initDone) {
            if (System.currentTimeMillis() - start > pythonInitTimeout) {
                try {
                    shutdownBridge();
                } catch (Throwable ignored) {}
                throw new IOException("Failed to start python process within timeout period");
            }
            try {
                WatchKey wk = watcher.poll(25, TimeUnit.MILLISECONDS);
                if (wk != null) {
                    for (WatchEvent<?> e : wk.pollEvents()) {
                        Object createdFilePath = e.context();
                        if (createdFilePath instanceof Path && ((Path)createdFilePath).endsWith("python_bridge_meta.done")) {
                            initDone = true;
                            break;
                        }
                    }
                }
            } catch (InterruptedException e) {
                try {
                    shutdownBridge();
                } catch (Throwable ignored) {}
                throw new RuntimeException("Failed to start python bridge", e);
            }
        }

        // Initiate java-side client server with info passed in the created python meta
        JsonNode bridgeMeta = new ObjectMapper().readTree(new File(this.envDir, "python_bridge_meta.json"));
        this.bridgeServer = new ClientServer.ClientServerBuilder()
                .authToken(bridgeMeta.get("token").asText())
                .javaPort(bridgeMeta.get("java_port").asInt())
                .javaAddress(InetAddress.getLoopbackAddress())
                .pythonPort(bridgeMeta.get("python_port").asInt())
                .pythonAddress(InetAddress.getLoopbackAddress())
                .build();
        this.bridgeServer.startServer();
        Logger.getGlobal().log(Level.INFO, "Successfully started python binding for " + entryPoint + " on " + InetAddress.getLoopbackAddress() + " port " + bridgeMeta.get("port").asInt());
    }

    public void shutdownBridge() {
        try {
            executor.getWatchdog().destroyProcess();
        } catch (Throwable ignored) {}
        deleteRecurs(this.envDir);
    }

    private void deleteRecurs(File parent) {
        File[] children = parent.listFiles();
        if (children != null) {
            for (File file : children) {
                deleteRecurs(file);
            }
        }
        parent.delete();
    }
}
