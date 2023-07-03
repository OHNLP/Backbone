package org.ohnlp.backbone.api.components.xlang.python;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import py4j.ClientServer;

import java.io.*;
import java.net.InetAddress;
import java.nio.file.*;
import java.util.Enumeration;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

public class PythonBridge<T> implements Serializable {
    private final long pythonInitTimeout = 30000; // TODO make this configurable
    private final String bundleName;
    private final String entryPoint;
    private final String entryClass;
    private final Class<T> pythonEntryPointClass;
    private final String envName;
    private transient File workDir;
    private transient File envDir;
    private transient DefaultExecutor executor;
    private transient ClientServer bridgeServer;
    private transient File launchFile;
    private transient OSType os;

    public PythonBridge(String bundleName, String envName, String entryPoint, String entryClass, Class<T> clazz) throws IOException {
        this.bundleName = bundleName;
        this.envName = envName;
        this.entryPoint = entryPoint;
        this.entryClass = entryClass;
        this.pythonEntryPointClass = clazz;
    }

    public void startBridge() throws IOException {
        detectOS();
        extractPythonResources();
        startServer();
    }

    private void detectOS() {
        String os = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);
        if (os.contains("windows")) {
            this.os = OSType.WINDOWS;
        } else if (os.contains("mac") || os.contains("darwin")) {
            this.os = OSType.MAC_DARWIN;
        } else if (!os.contains("linux")) {
            this.os = OSType.LINUX;
        } else {
            this.os = OSType.OTHER;
        }
    }


    public T getPythonEntryPoint() {
        return (T) this.bridgeServer.getPythonServerEntryPoint(new Class[]{this.pythonEntryPointClass});
    }

    /*
     * Because Backbone bundles everything into a master jar file as part of its build process, we need to extract this into
     * a tmp folder to properly execute the python process with associated dependencies
     */
    private void extractPythonResources() {
        String name = this.getClass().getSimpleName() + "_tmp_" + UUID.randomUUID();
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        this.workDir = new File(tmpDir, name);
        this.workDir.mkdirs();
        // Extract both python launcher script
        try (InputStream launcherPy = this.getClass().getResourceAsStream("/backbone_launcher.py")) {
            this.launchFile = new File(this.workDir, "backbone_launcher_" + System.currentTimeMillis() + ".py");
            Files.copy(launcherPy, this.launchFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Extract the python script itself
        try (ZipInputStream zis = new ZipInputStream(getClass().getResourceAsStream("/python_modules/" + this.bundleName))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                String pathRelative = entry.getName();
                File pathInTmp = new File(this.workDir, pathRelative);
                byte[] contents = zis.readAllBytes();
                try (FileOutputStream fos = new FileOutputStream(pathInTmp)) {
                    fos.write(contents);
                    fos.flush();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Extract shared python resources
        final File jarFile = new File(getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
        try (JarFile jar = new JarFile(jarFile)){
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                if (entry.isDirectory()) {
                    continue;
                }
                if (!entry.getName().contains("python_resources")) {
                    continue;
                }
                String pathRelative = entry.getName();
                // -- truncate the python_resources part out of the output path
                pathRelative = pathRelative.replaceAll("^" + File.separator + "?python_resources" + File.separator, "");
                File pathInTmp = new File(this.workDir, pathRelative);
                byte[] contents = jar.getInputStream(entry).readAllBytes();
                try (FileOutputStream fos = new FileOutputStream(pathInTmp)) {
                    fos.write(contents);
                    fos.flush();
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Instantiate Conda Env
        String localEnvName = "env_" + UUID.randomUUID().getLeastSignificantBits();
        this.envDir = new File(this.workDir, localEnvName);
        if (this.envName == null) {
            // No pre-bundled environment... create conda environment live
            String condaCmd = "conda env create -n $1 -f environment.yml --prefix $2".replace("$1", localEnvName).replace("$2", this.workDir.getAbsolutePath());
            CommandLine cmdLine = CommandLine.parse(condaCmd);
            this.executor = new DefaultExecutor();
            this.executor.setWorkingDirectory(this.workDir);
            try {
                int result = this.executor.execute(cmdLine); // Blocking wait
                if (result != 0) {
                    throw new IOException("Conda environment instantiation failed with code " + result + " please check job logs");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            this.envDir.mkdirs();
            // Extract the packaged conda env TODO: instantiate live instead if not standalone and can be run offline
            String osPath = "linux";
            if (this.os.equals(OSType.WINDOWS)) {
                osPath = "win32";
            } else {
                if (this.os.equals(OSType.MAC_DARWIN)) {
                    osPath = "darwin";
                } else if (!this.os.equals(OSType.LINUX)) {
                    osPath = "unix";
                }
            }
            File envTar;
            try {
                InputStream envTarStream = this.getClass().getResourceAsStream("/python_envs/$1/$2.tar.gz".replace("$1", osPath).replace("$2", this.envName));
                if (envTarStream == null) {
                    if (osPath.equals("unix")) {
                        throw new IOException("Could not find declared bundled/offline environment " + this.envName + "for OS " + osPath + ", check your /python_envs folder or change settings to online/dynamic resolution mode.");
                    } else {
                        Logger.getGlobal().log(Level.WARNING, "Could not find declared bundled/offline environment " + this.envName + " for OS " + osPath + ", falling back to unix environment, unexpected behaviour may occur");
                        envTarStream = this.getClass().getResourceAsStream("/python_envs/$1/$2.tar.gz".replace("$1", "unix").replace("$2", this.envName));
                        if (envTarStream == null) {
                            throw new IOException("Could not find declared bundled/offline environment " + this.envName + "for OS " + osPath + ", check your /python_envs folder or change settings to online/dynamic resolution mode.");
                        }
                    }
                }
                envTar = new File(this.workDir, "environment.tar.gz");
                Files.copy(envTarStream, envTar.toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            TarGZipUnArchiver unarchiver = new TarGZipUnArchiver();
            unarchiver.setSourceFile(envTar);
            unarchiver.setDestDirectory(this.envDir);
            unarchiver.extract();
        }
    }

    /*
     * Spins up the python process/py4j bridge
     */
    private void startServer() throws IOException {
        // Create sentinel watcher for python process initialization
        WatchService watcher = FileSystems.getDefault().newWatchService();
        this.workDir.toPath().register(watcher, ENTRY_CREATE);
        // Launch the python process
        // TODO this does NOT activate conda/fix prefixes, which may cause issues with some libraries
        // TODO Consider doing the full conda-unpack routine in a future release
        // See: https://conda.github.io/conda-pack/ under "Commandline Usage"
        String cmd = String.join(" ",
                new File(new File(this.envDir, "bin"), "python").getAbsolutePath(),
                "backbone_launcher.py",
                entryPoint,
                entryClass,
                this.pythonEntryPointClass.equals(PythonBackbonePipelineComponent.class) ? "component" : "dofn");
        CommandLine cmdLine = CommandLine.parse(cmd);
        this.executor = new DefaultExecutor();
        this.executor.setWorkingDirectory(this.workDir);
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
                    } catch (Throwable ignored) {
                    }
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
                } catch (Throwable ignored) {
                }
                throw new IOException("Failed to start python process within timeout period");
            }
            try {
                WatchKey wk = watcher.poll(25, TimeUnit.MILLISECONDS);
                if (wk != null) {
                    for (WatchEvent<?> e : wk.pollEvents()) {
                        Object createdFilePath = e.context();
                        if (createdFilePath instanceof Path && ((Path) createdFilePath).endsWith("python_bridge_meta.done")) {
                            initDone = true;
                            break;
                        }
                    }
                }
            } catch (InterruptedException e) {
                try {
                    shutdownBridge();
                } catch (Throwable ignored) {
                }
                throw new RuntimeException("Failed to start python bridge", e);
            }
        }

        // Initiate java-side client server with info passed in the created python meta
        JsonNode bridgeMeta = new ObjectMapper().readTree(new File(this.workDir, "python_bridge_meta.json"));
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
        } catch (Throwable ignored) {
        }
        deleteRecurs(this.workDir);
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

    private enum OSType {
        WINDOWS,
        MAC_DARWIN,
        LINUX,
        OTHER
    }
}
