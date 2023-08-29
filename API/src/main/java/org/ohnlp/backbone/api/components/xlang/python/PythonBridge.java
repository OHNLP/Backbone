package org.ohnlp.backbone.api.components.xlang.python;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.exec.*;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import py4j.ClientServer;

import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

public class PythonBridge<T> implements Serializable {

    private static final ConcurrentHashMap<String, CompletableFuture<PythonEnvironment>> JVM_ENVIRONMENTS_BY_BUNDLE_ID
            = new ConcurrentHashMap<>();
    private static final ConcurrentLinkedQueue<DefaultExecutor> JVM_SUBPROCESS_REFERENCES = new ConcurrentLinkedQueue<>();
    public static boolean CLEANUP_ENVS_ON_SHUTDOWN = true;
    private final long pythonInitTimeout = 300000; // TODO make this configurable
    private final String bundleIdentifier;
    private final String entryPoint;
    private final String entryClass;
    private final Class<T> pythonEntryPointClass;
    private final File tmpDir;
    private transient DefaultExecutor executor;
    private transient ClientServer bridgeServer;
    private transient File launchFile;
    private transient OSType os;

    public PythonBridge(File tmpDir, String bundleName, String entryPoint, String entryClass, Class<T> clazz) throws IOException {
        this.tmpDir = tmpDir;
        this.bundleIdentifier = bundleName;
        this.entryPoint = entryPoint;
        this.entryClass = entryClass;
        this.pythonEntryPointClass = clazz;
    }

    public void startBridge() throws IOException {
        detectOS();
        extractPythonResourcesIfNotExists();
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
    private void extractPythonResourcesIfNotExists() {
        CompletableFuture<PythonEnvironment> env = new CompletableFuture<>();
        synchronized (JVM_ENVIRONMENTS_BY_BUNDLE_ID) {
            if (JVM_ENVIRONMENTS_BY_BUNDLE_ID.contains(this.bundleIdentifier)) {
                try {
                    // Environment already exists and/or is initializing, just wait for completion and return
                    JVM_ENVIRONMENTS_BY_BUNDLE_ID.get(this.bundleIdentifier).get();
                    return;
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            } else {
                // Environment does not yet exist, populate JVM_ENVIRONMENTS_BY_BUNDLE_ID to indicate that it
                // is being created
                JVM_ENVIRONMENTS_BY_BUNDLE_ID.put(this.bundleIdentifier, env);
            }
        }
        String name = "ohnlptk_" + this.bundleIdentifier+ "_" + System.currentTimeMillis();
        if (!CLEANUP_ENVS_ON_SHUTDOWN) {
            name = this.bundleIdentifier;
        }
        File workDir = new File(tmpDir, name);
        workDir.mkdirs();
        //
        boolean initModule = true;
        boolean initEnv = true;
        String cachedModuleChecksum = "";
        String cachedEnvChecksum = "";
        try {
            File hashFile = new File(workDir, "modulehash.txt");
            if (hashFile.exists()) {
                cachedModuleChecksum = Files.readAllLines(hashFile.toPath()).get(0);
            }
            File envFile = new File(workDir, "environment.yml");
            if (envFile.exists()) {
                byte[] data = Files.readAllBytes(Paths.get(new File(workDir, "environment.yml").getPath()));
                byte[] hash = MessageDigest.getInstance("MD5").digest(data);
                cachedEnvChecksum = new BigInteger(1, hash).toString(16);
            }
        } catch (IOException | NoSuchAlgorithmException e) {
            env.completeExceptionally(e);
            return;
        }
        if (!CLEANUP_ENVS_ON_SHUTDOWN) {
            boolean reinit = false;
            try {
                InputStream bundleIS = findBundle();
                byte[] data = bundleIS.readAllBytes();
                String currModuleChecksum = new BigInteger(1, MessageDigest.getInstance("MD5").digest(data)).toString(16);
                if (currModuleChecksum.equals(cachedModuleChecksum)) {
                    initModule = false;
                } else {
                    reinit = true;
                    Logger.getGlobal().log(Level.INFO, "Hash of Python Module " + this.bundleIdentifier + " Changed, Invalidating Module Cache");
                    cachedModuleChecksum = currModuleChecksum;
                }
            } catch (Throwable t) {
                reinit = true;
            }
            if (reinit) {
                deleteRecurs(workDir);
                workDir.mkdirs();
            }
        }

        // Extract python launcher script
        try (InputStream launcherPy = this.getClass().getResourceAsStream("/backbone_launcher.py")) {
            this.launchFile = new File(workDir, "backbone_launcher.py");
            Files.copy(launcherPy, this.launchFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            env.completeExceptionally(e);
            return;
        }
        // Identify the correct python environment bundle and copy resources if module data needs to be replaced
        if (initModule) {
            try {
                InputStream is = findBundle();
                if (is == null) {
                    env.completeExceptionally(new IllegalArgumentException("Could not find module " + this.bundleIdentifier));
                    return;
                }
                ZipInputStream zis = new ZipInputStream(is);
                // - Now actually copy the resources over
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    if (entry.isDirectory()) {
                        continue;
                    }
                    String pathRelative = entry.getName();
                    File pathInTmp = new File(workDir, pathRelative);
                    byte[] contents = zis.readAllBytes();
                    try (FileOutputStream fos = new FileOutputStream(pathInTmp)) {
                        fos.write(contents);
                        fos.flush();
                    }
                }
                zis.close();
            } catch (IOException e) {
                env.completeExceptionally(e);
                return;
            }
        }
        // Extract shared python resources
        final File jarFile = new File(getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
        try (JarFile jar = new JarFile(jarFile)) {
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
                File pathInTmp = new File(workDir, pathRelative);
                byte[] contents = jar.getInputStream(entry).readAllBytes();
                try (FileOutputStream fos = new FileOutputStream(pathInTmp)) {
                    fos.write(contents);
                    fos.flush();
                }
            }

        } catch (IOException e) {
            env.completeExceptionally(e);
            return;
        }
        // Check if env reinstantiation necessary by comparing environment.yml hashes
        try {
            byte[] data = Files.readAllBytes(Paths.get(new File(workDir, "environment.yml").getPath()));
            byte[] hash = MessageDigest.getInstance("MD5").digest(data);
            String currEnvChecksum = new BigInteger(1, hash).toString(16);
            if (currEnvChecksum.equals(cachedEnvChecksum)) {
                initEnv = false;
            } else {
                Logger.getGlobal().log(Level.INFO, "environment.yml checksum changed, reinitializing");
                File envDir = new File(workDir, "env");
                if (envDir.exists()) {
                    deleteRecurs(envDir);
                }
            }
        } catch (Throwable e) {
            env.completeExceptionally(e);
            return;
        }
        // Instantiate Conda Env
        String localEnvName = "env";
        File envDir = new File(workDir, localEnvName);
        envDir.mkdirs();
        if (initEnv) {
            // Extract the packaged conda env
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
                        Logger.getGlobal().log(Level.INFO, "Could not find bundled/offline environment " + this.bundleIdentifier + " for OS " + osPath + ", attempting online/dynamic environment resolution");
                        dynamicallyResolveEnvironment(localEnvName, workDir);
                    } else {
                        if (!osPath.equals("win32")) {
                            Logger.getGlobal().log(Level.INFO, "Could not find bundled/offline environment " + this.bundleIdentifier + " for OS " + osPath + ", attempting to fall back to unix environment, unexpected behaviour may occur");
                            envTarStream = findEnv("unix");
                            if (envTarStream == null) {
                                Logger.getGlobal().log(Level.INFO, "Could not find bundled/offline environment " + this.bundleIdentifier + " for OS Unix, attempting online/dynamic environment resolution");
                                dynamicallyResolveEnvironment(localEnvName, workDir);
                            }
                        } else {
                            Logger.getGlobal().log(Level.INFO, "Could not find bundled/offline environment " + this.bundleIdentifier + " for OS " + osPath + ", win32 cannot fall back to unix, so attempting online/dynamic environment resolution");
                            dynamicallyResolveEnvironment(localEnvName, workDir);
                        }
                    }
                }
                if (envTarStream != null) {
                    envTar = new File(workDir, "environment.tar.gz");
                    Files.copy(envTarStream, envTar.toPath(), StandardCopyOption.REPLACE_EXISTING);
                    TarGZipUnArchiver unarchiver = new TarGZipUnArchiver();
                    unarchiver.setSourceFile(envTar);
                    unarchiver.setDestDirectory(envDir);
                    unarchiver.extract();
                }
            } catch (IOException e) {
                env.completeExceptionally(e);
                return;
            }
        }
        ObjectWriter ow = new ObjectMapper().writer();
        try (FileWriter fw = new FileWriter(new File(workDir, "modulehash.txt"))){
            fw.write(cachedModuleChecksum);
            fw.flush();
        } catch (IOException ignored) {
        }
        // Write new checksums
        env.complete(new PythonEnvironment(workDir, envDir));
    }

    private InputStream findBundle() throws IOException {
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
                            return new FileInputStream(f);
                        }
                    }
                }
                return null;
            } else {
                JsonNode node = new ObjectMapper().readTree(is);
                return getClass().getResourceAsStream("/python_modules/" + node.get(this.bundleIdentifier).asText());
            }
        }
    }

    private InputStream findEnv(String os) throws FileNotFoundException {
        // First, try classpath
        InputStream ret = this.getClass().getResourceAsStream("/python_envs/$1/$2.tar.gz".replace("$1", os).replace("$2", this.bundleIdentifier));
        if (ret == null) {
            File envFile = new File(new File("python_envs", os), this.bundleIdentifier + ".tar.gz");
            if (envFile.exists()) {
                ret = new FileInputStream(envFile);
            }
        }
        return ret;
    }

    private void dynamicallyResolveEnvironment(String localEnvName, File workDir) {
        // No pre-bundled environment... create conda environment live
        List<String> command = new ArrayList<>();
        if (this.os.equals(OSType.WINDOWS)) { // run via a cmd call instead of directly
            command.addAll(Arrays.asList("cmd.exe", "/c"));
        }
        command.add("conda");
        command.addAll(Arrays.asList("env", "create", "-f", "environment.yml", "--prefix", localEnvName));
        CommandLine cmdLine = CommandLine.parse(String.join(" ", command));
        this.executor = new DefaultExecutor(); // Can be safely set because conda is guaranteed to exit before python process is started
        this.executor.setWatchdog(new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT));
        this.executor.setProcessDestroyer(new ShutdownHookProcessDestroyer());
        JVM_SUBPROCESS_REFERENCES.add(this.executor);
        this.executor.setWorkingDirectory(workDir);
        try {
            int result = this.executor.execute(cmdLine);
            if (result != 0) {
                throw new IOException("Conda environment instantiation failed with code " + result + " please check job logs");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * Spins up the python process/py4j bridge
     */
    private void startServer() throws IOException {
        PythonEnvironment env;
        try {
            env = JVM_ENVIRONMENTS_BY_BUNDLE_ID.get(this.bundleIdentifier).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        // Create sentinel watcher for python process initialization
        WatchService watcher = FileSystems.getDefault().newWatchService();
        env.workDir.toPath().register(watcher, ENTRY_CREATE);
        String id = UUID.randomUUID().toString();
        // Launch the python process
        // TODO this does NOT activate conda/fix prefixes, which may cause issues with some libraries
        // TODO Consider doing the full conda-unpack routine in a future release
        // See: https://conda.github.io/conda-pack/ under "Commandline Usage"
        String pythonPath = os.equals(OSType.WINDOWS) ? new File(env.envDir, "python.exe").getAbsolutePath() : new File(new File(env.envDir, "bin"), "python").getAbsolutePath();
        String cmd = String.join(" ",
                pythonPath,
                this.launchFile.getName(),
                entryPoint,
                entryClass,
                this.pythonEntryPointClass.equals(PythonBackbonePipelineComponent.class) ? "component" : "dofn",
                id);
        CommandLine cmdLine = CommandLine.parse(cmd);
        this.executor = new DefaultExecutor();
        this.executor.setWatchdog(new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT));
        this.executor.setWorkingDirectory(env.workDir);
        this.executor.setProcessDestroyer(new ShutdownHookProcessDestroyer());
        JVM_SUBPROCESS_REFERENCES.add(this.executor);
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
        File watchFile = new File(env.workDir, "python_bridge_meta_" + id + ".done");
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
        JsonNode bridgeMeta = new ObjectMapper().readTree(new File(env.workDir, "python_bridge_meta_" + id + ".json"));
        this.bridgeServer = new ClientServer.ClientServerBuilder()
                .authToken(bridgeMeta.get("token").asText())
                .javaPort(bridgeMeta.get("java_port").asInt())
                .javaAddress(InetAddress.getLoopbackAddress())
                .pythonPort(bridgeMeta.get("python_port").asInt())
                .pythonAddress(InetAddress.getLoopbackAddress())
                .build();
        this.bridgeServer.startServer();
        Logger.getGlobal().log(Level.INFO, "Successfully started python binding for " + entryPoint + " on " + InetAddress.getLoopbackAddress() + " port " + bridgeMeta.get("python_port").asInt());
    }

    public synchronized final void shutdownBridge() {
        try {
            executor.getWatchdog().destroyProcess();
        } catch (Throwable ignored) {
        }
    }

    private static void deleteRecurs(File parent) {
        if (!parent.exists()) {
            return;
        }
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

    private static final class PythonEnvironment {
        private File workDir;
        private File envDir;

        public PythonEnvironment(File workDir, File envDir) {
            this.workDir = workDir;
            this.envDir = envDir;
        }

        public File getWorkDir() {
            return workDir;
        }

        public File getEnvDir() {
            return envDir;
        }
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Cleanup Threads
            for (DefaultExecutor executor : JVM_SUBPROCESS_REFERENCES) {
                try {
                    executor.getWatchdog().destroyProcess();
                } catch (Throwable ignored) {}
            }
            // Cleanup Envs
            if (CLEANUP_ENVS_ON_SHUTDOWN) {
                for (CompletableFuture<PythonEnvironment> env : JVM_ENVIRONMENTS_BY_BUNDLE_ID.values()) {
                    try {
                        deleteRecurs(env.get().workDir);
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }));
    }
}
