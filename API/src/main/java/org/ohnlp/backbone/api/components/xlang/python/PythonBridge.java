package org.ohnlp.backbone.api.components.xlang.python;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.codec.binary.Hex;
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
import java.security.SecureRandom;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class PythonBridge<T> implements Serializable {
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
        try (ZipInputStream zis = new ZipInputStream(getClass().getResourceAsStream("/python_modules/" + this.bundleName))) {
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

    /*
     * Spins up the python process/py4j bridge
     */
    private void startServer() throws IOException {
        // Generate a common secret for bridge communications
        SecureRandom rnd = new SecureRandom();
        byte[] secretBytes = new byte[16];
        rnd.nextBytes(secretBytes);
        String secret = Hex.encodeHexString(secretBytes);

        // Initiate java-side client server without a defined port
        this.bridgeServer = new ClientServer.ClientServerBuilder()
                .authToken(secret)
                .javaPort(0)
                .javaAddress(InetAddress.getLoopbackAddress())
                .build();
        this.bridgeServer.startServer();
        // Resolve connection info and write to temp folder/file
        int port = this.bridgeServer.getJavaServer().getListeningPort();
        ObjectNode connInfo = JsonNodeFactory.instance.objectNode();
        connInfo.put("port", port);
        connInfo.put("token", secret);
        new ObjectMapper().writer().writeValue(new File(envDir, "vars.json"), connInfo);
        Logger.getGlobal().log(Level.INFO, "Starting python binding for " + entryPoint + " on " + InetAddress.getLoopbackAddress() + " port " + port);
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
                    // TODO
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
