package org.ohnlp.backbone.api.components.xlang.python;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import py4j.ClientServer;

import java.io.*;
import java.net.InetAddress;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

public class PythonBridge<T> implements Serializable {
    private final long pythonInitTimeout = 30000; // TODO make this configurable
    private final String bundleIdentifier;
    private final String entryPoint;
    private final String entryClass;
    private final Class<T> pythonEntryPointClass;
    private transient File workDir;
    private transient File envDir;
    private transient DefaultExecutor executor;
    private transient ClientServer bridgeServer;
    private transient File launchFile;
    private transient OSType os;

    public PythonBridge(String bundleName, String entryPoint, String entryClass, Class<T> clazz) throws IOException {
        this.bundleIdentifier = bundleName;
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
        // Identify the correct python environment bundle and copy resources.
        try {
            ZipInputStream zis = null;
            // - First try to scan classpath for registered_python_modules.json
            try (InputStream is = getClass().getResourceAsStream("/registered_modules.json")) {
                if (is == null) {
                    // This is being run in a non-packaged environment, do a scan through python_modules
                    Logger.getGlobal().warning("Attempting instantiation of python bridge from a Non-Packaged Environment. If this is occurring outside of pipeline configuration/setup, unexpected behaviour may occur on deployment. Falling back to scanning the python_modules folder");
                    for (File f : new File("python_modules").listFiles()) {
                        try (ZipFile zip = new ZipFile(f)) {
                            ZipEntry entry = zip.getEntry("backbone_module.json");
                            JsonNode on = new ObjectMapper().readTree(zip.getInputStream(entry));
                            if (on.has("module_identifier") && on.get("module_identifier").asText().equals(this.bundleIdentifier)) {
                                zis = new ZipInputStream(new FileInputStream(f));
                                break;
                            }
                        }
                    }
                } else {
                    JsonNode node = new ObjectMapper().readTree(is);
                    zis = new ZipInputStream(getClass().getResourceAsStream("/python_modules/" + node.get(this.bundleIdentifier).asText()));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            // - Now actually copy the resources over
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
            zis.close();
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
        String localEnvName = "env";
        this.envDir = new File(this.workDir, localEnvName);
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
            InputStream envTarStream = findEnv(osPath);
            if (envTarStream == null) {
                if (osPath.equals("unix")) {
                    Logger.getGlobal().log(Level.INFO, "Could not find bundled/offline environment " + this.bundleIdentifier + " for OS " + osPath + ", attempting online/dynamic environment resolutions.");
                    dynamicallyResolveEnvironment(localEnvName);
                } else {
                    Logger.getGlobal().log(Level.INFO, "Could not find bundled/offline environment " + this.bundleIdentifier + " for OS " + osPath + ", attempting to fall back to unix environment, unexpected behaviour may occur");
                    envTarStream = findEnv("unix");
                    if (envTarStream == null) {
                        Logger.getGlobal().log(Level.INFO, "Could not find bundled/offline environment " + this.bundleIdentifier + " for OS Unix, attempting online/dynamic environment resolutions.");
                        dynamicallyResolveEnvironment(localEnvName);
                    }
                }
            }
            if (envTarStream != null) {
                envTar = new File(this.workDir, "environment.tar.gz");
                Files.copy(envTarStream, envTar.toPath(), StandardCopyOption.REPLACE_EXISTING);
                TarGZipUnArchiver unarchiver = new TarGZipUnArchiver();
                unarchiver.setSourceFile(envTar);
                unarchiver.setDestDirectory(this.envDir);
                unarchiver.extract();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private InputStream findEnv(String os) throws FileNotFoundException {
        // First, try classpath
        InputStream ret = this.getClass().getResourceAsStream("/python_envs/$1/$2".replace("$1", os).replace("$2", this.bundleIdentifier));
        if (ret == null) {
            File envFile = new File(new File("python_envs", os), this.bundleIdentifier);
            if (envFile.exists()) {
                ret = new FileInputStream(envFile);
            }
        }
        return ret;
    }

    private void dynamicallyResolveEnvironment(String localEnvName) {
        // No pre-bundled environment... create conda environment live
        List<String> command = new ArrayList<>();
        if (this.os.equals(OSType.WINDOWS)) { // run via a cmd call instead of directly
            command.addAll(Arrays.asList("cmd.exe", "/c"));
        }
        command.add("conda");
        command.addAll(Arrays.asList("env", "create", "-f", "environment.yml", "--prefix", localEnvName));
        ProcessBuilder pb = new ProcessBuilder().directory(this.workDir).command(command).inheritIO();
        try {
            int result = pb.start().waitFor();
            if (result != 0) {
                throw new IOException("Conda environment instantiation failed with code " + result + " please check job logs");
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
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
        String pythonPath = os.equals(OSType.WINDOWS) ? new File(this.envDir, "python.exe").getAbsolutePath() : new File(new File(this.envDir, "bin"), "python").getAbsolutePath();
        String cmd = String.join(" ",
                pythonPath,
                this.launchFile.getName(),
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
        File watchFile = new File(this.workDir, "python_bridge_meta.done");
        while (!initDone) {
            if (System.currentTimeMillis() - start > pythonInitTimeout) {
                try {
                    shutdownBridge();
                } catch (Throwable ignored) {
                }
                throw new IOException("Failed to start python process within timeout period");
            }
            try {
                if (!watchFile.exists()) {
                    Thread.sleep(1000);
                } else {
                    break;
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
        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdownBridge()));
        Logger.getGlobal().log(Level.INFO, "Successfully started python binding for " + entryPoint + " on " + InetAddress.getLoopbackAddress() + " port " + bridgeMeta.get("python_port").asInt());
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
