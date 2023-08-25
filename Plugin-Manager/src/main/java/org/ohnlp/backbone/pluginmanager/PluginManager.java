package org.ohnlp.backbone.pluginmanager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class PluginManager {

    public static void main(String... args) throws IOException {
        File hashFile = new File("bin", "last_modified.json");

        ObjectNode hashes = hashFile.exists() ? (ObjectNode) new ObjectMapper().readTree(hashFile) : JsonNodeFactory.instance.objectNode();
        Set<String> builds = new HashSet<>();
        if (args.length > 0) {
            builds = Arrays.stream(args).map(String::toLowerCase).collect(Collectors.toSet());
        }
        List<File> modules = Arrays.asList(Objects.requireNonNull(new File("modules").listFiles()));
        List<File> configs = Arrays.asList(Objects.requireNonNull(new File("configs").listFiles()));
        List<File> resources = Arrays.asList(Objects.requireNonNull(new File("resources").listFiles()));
        List<File> pythonModules = new File("python_modules").exists() ? Arrays.asList(Objects.requireNonNull(new File("python_modules").listFiles())) : Collections.emptyList();
        List<File> pythonResources = new File("python_resources").exists() ? Arrays.asList(Objects.requireNonNull(new File("python_resources").listFiles())) : Collections.emptyList();
        List<File> pythonEnvs = new File("python_envs").exists() ? Arrays.asList(Objects.requireNonNull(new File("python_envs").listFiles())) : Collections.emptyList();
        for (File f : new File("bin").listFiles()) {
            if (!f.isDirectory()) {
                if (f.getName().startsWith("Backbone-Core") && !f.getName().endsWith("Packaged.jar")) {
                    File source = f;
                    String type = f.getName().substring(14, f.getName().length() - 4);
                    if (!hashes.has(type)) {
                        hashes.set(type, JsonNodeFactory.instance.objectNode());
                    }
                    ObjectNode typeHash = (ObjectNode) hashes.get(type);
                    if (builds.size() == 0 || builds.contains(type.toLowerCase(Locale.ROOT))) {
                        System.out.println("Repackaging files changed since last packaging of " + type + " platform-specific JAR:");
                        File target = new File("bin/Backbone-Core-" + type + "-Packaged.jar");
                        // Do a complete rebuild in update in case of class conflicts between submodules
                        // TODO actually check this in the future (i.e., by preventing overwrites in org.ohnlp.backbone
                        //  classes during install()
                        boolean invalidateAllHashes = false;
                        boolean updated = false;
                        Set<String> touched = new HashSet<>();
                        if (!checkAndUpdateMD5ChecksumRegistry(false, typeHash, f, touched)) {
                            System.out.println("- " + getRelativePath(f));
                            Files.copy(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
                            invalidateAllHashes = true;
                            updated = true;
                        }
                        updated = install(invalidateAllHashes, target, typeHash, modules, configs, resources, touched, pythonModules, pythonResources, pythonEnvs) || updated;
                        if (!updated) {
                            System.out.println("No changed files found");
                        } else {
                            System.out.println("Successfully Packaged Platform-Specific JAR: " + target.getAbsolutePath());
                        }
                    }
                }
            }
        }
        // Write hashes out
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(hashFile, hashes);
        System.out.println("Packaging complete!");
    }

    private static boolean checkAndUpdateMD5ChecksumRegistry(boolean invalidateAllHashes, ObjectNode hashRegistry, File f, Set<String> touched) {
        try {
            byte[] data = Files.readAllBytes(Paths.get(f.getPath()));
            byte[] hash = MessageDigest.getInstance("MD5").digest(data);
            String checksum = new BigInteger(1, hash).toString(16);
            String relativePath = getRelativePath(f);
            touched.add(relativePath);
            if (invalidateAllHashes
                    || !hashRegistry.has(relativePath)
                    || !hashRegistry.get(relativePath).asText().equals(checksum)) {
                hashRegistry.put(relativePath, checksum);
                return false;
            } else {
                return true;
            }
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Packs an OHNLP backbone executable with the specified set of modules,  configurations, and resources
     *
     * @param invalidateAllHashes Whether to ignore/replace all hashes
     * @param target              The target file for packaging/into which items should be installed
     * @param hashRegistry        A registry of currently
     * @param modules             A list of module jar files to install
     * @param configurations      A set of configuration files to install
     * @param resources           A set of resource directories to install
     * @param touched
     */
    public static boolean install(boolean invalidateAllHashes, File target, ObjectNode hashRegistry, List<File> modules, List<File> configurations, List<File> resources, Set<String> touched, List<File> pythonModules, List<File> pythonResources, List<File> pythonEnvs) {
        AtomicBoolean updated = new AtomicBoolean(false);
        Map<String, String> env = new HashMap<>();
        env.put("create", "false");
        try (FileSystem fs = FileSystems.newFileSystem(target.toPath(), PluginManager.class.getClassLoader())) {
            // ==== JAVA INSTALL START =====
            // Configs
            Files.createDirectories(fs.getPath("/configs"));
            for (File config : configurations) {
                if (!checkAndUpdateMD5ChecksumRegistry(invalidateAllHashes, hashRegistry, config, touched)) {
                    updated.set(true);
                    System.out.println("- " + getRelativePath(config));
                    Files.copy(config.toPath(), fs.getPath("/configs/" + config.getName()), StandardCopyOption.REPLACE_EXISTING);
                }
            }
            // Modules
            for (File module : modules) {
                if (!checkAndUpdateMD5ChecksumRegistry(invalidateAllHashes, hashRegistry, module, touched)) {
                    updated.set(true);
                    System.out.println("- " + getRelativePath(module));
                    try (FileSystem srcFs = FileSystems.newFileSystem(module.toPath(), PluginManager.class.getClassLoader())) {
                        Path srcRoot = srcFs.getPath("/");
                        Files.walkFileTree(srcRoot, new SimpleFileVisitor<Path>() {
                            @Override
                            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                                String fName = path.getFileName().toString().toLowerCase(Locale.ROOT);
                                if (fName.endsWith(".sf") || fName.endsWith(".dsa") || fName.endsWith(".rsa")) {
                                    return FileVisitResult.CONTINUE;
                                }
                                Path tgtPath = fs.getPath(path.toString());
                                Files.createDirectories(tgtPath.getParent());
                                Files.copy(path, tgtPath, StandardCopyOption.REPLACE_EXISTING);
                                return FileVisitResult.CONTINUE;
                            }
                        });
                    }
                }
            }
            Files.createDirectories(fs.getPath("/configs"));
            for (File config : configurations) {
                if (!checkAndUpdateMD5ChecksumRegistry(invalidateAllHashes, hashRegistry, config, touched)) {
                    updated.set(true);
                    System.out.println("- " + getRelativePath(config));
                    Files.copy(config.toPath(), fs.getPath("/configs/" + config.getName()), StandardCopyOption.REPLACE_EXISTING);
                }
            }
            Files.createDirectories(fs.getPath("/resources"));
            // Always update all resources
            for (File resource : resources) {
                String resourceDir = resource.getParentFile().toPath().toAbsolutePath().toString();
                if (resource.isDirectory()) {
                    // Recursively find all files in directory (up to arbitrary max depth of 999)
                    Files.find(resource.toPath(), 999, (p, bfa) -> bfa.isRegularFile()).forEach(p -> {
                        try {
                            if (!checkAndUpdateMD5ChecksumRegistry(invalidateAllHashes, hashRegistry, p.toFile(), touched)) {
                                System.out.println("- " + getRelativePath(p.toFile()));
                                updated.set(true);
                                Path filePath = fs.getPath(p.toAbsolutePath().toString().replace(resourceDir, "/resources"));
                                Files.createDirectories(filePath.getParent());
                                Files.copy(p, filePath, StandardCopyOption.REPLACE_EXISTING);
                            }
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    });
                } else {
                    if (!checkAndUpdateMD5ChecksumRegistry(invalidateAllHashes, hashRegistry, resource, touched)) {
                        System.out.println("- " + getRelativePath(resource));
                        updated.set(true);
                        Files.copy(resource.toPath(), fs.getPath("/resources/" + resource.getName()), StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            }
            // ==== PYTHON INSTALL START =====
            // Modules
            Files.createDirectories(fs.getPath("/python_modules"));
            Map<String, String> registeredModules = new HashMap<>();
            for (File module : pythonModules) {
                if (!checkAndUpdateMD5ChecksumRegistry(invalidateAllHashes, hashRegistry, module, touched)) {
                    updated.set(true);
                    System.out.println("- " + getRelativePath(module));
                    Files.copy(module.toPath(), fs.getPath("/python_modules/" + module.getName()), StandardCopyOption.REPLACE_EXISTING);
                }
                try (ZipFile zf = new ZipFile(module)) {
                    ZipEntry e = zf.getEntry("backbone_module.json");
                    if (e != null) {
                        JsonNode def = new ObjectMapper().readTree(zf.getInputStream(e));
                        registeredModules.put(def.get("module_identifier").asText(), module.getName());
                    }
                }
            }
            if (updated.get()) {
                File moduleRegistrationFile = new File("registered_modules.json");
                new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(moduleRegistrationFile, registeredModules);
                Files.copy(moduleRegistrationFile.toPath(), fs.getPath("/registered_modules.json"), StandardCopyOption.REPLACE_EXISTING);
                moduleRegistrationFile.delete();
            }
            Files.createDirectories(fs.getPath("/python_resources"));
            // Resources
            for (File resource : pythonResources) {
                String resourceDir = resource.getParentFile().toPath().toAbsolutePath().toString();
                if (resource.isDirectory()) {
                    // Recursively find all files in directory (up to arbitrary max depth of 999)
                    Files.find(resource.toPath(), 999, (p, bfa) -> bfa.isRegularFile()).forEach(p -> {
                        try {
                            if (!checkAndUpdateMD5ChecksumRegistry(invalidateAllHashes, hashRegistry, p.toFile(), touched)) {
                                System.out.println("- " + getRelativePath(p.toFile()));
                                updated.set(true);
                                Path filePath = fs.getPath(p.toAbsolutePath().toString().replace(resourceDir, "/python_resources"));
                                Files.createDirectories(filePath.getParent());
                                Files.copy(p, filePath, StandardCopyOption.REPLACE_EXISTING);
                            }
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    });
                } else {
                    if (!checkAndUpdateMD5ChecksumRegistry(invalidateAllHashes, hashRegistry, resource, touched)) {
                        System.out.println("- " + getRelativePath(resource));
                        updated.set(true);
                        Files.copy(resource.toPath(), fs.getPath("/python_resources/" + resource.getName()), StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            }
            // Envs
            for (File envFile : pythonEnvs) {
                String envDir = envFile.getParentFile().toPath().toAbsolutePath().toString();
                if (envFile.isDirectory()) {
                    // Recursively find all files in directory (up to arbitrary max depth of 999)
                    Files.find(envFile.toPath(), 999, (p, bfa) -> bfa.isRegularFile()).forEach(p -> {
                        try {
                            if (!checkAndUpdateMD5ChecksumRegistry(invalidateAllHashes, hashRegistry, p.toFile(), touched)) {
                                System.out.println("- " + getRelativePath(p.toFile()));
                                updated.set(true);
                                Path filePath = fs.getPath(p.toAbsolutePath().toString().replace(envDir, "/python_envs"));
                                Files.createDirectories(filePath.getParent());
                                Files.copy(p, filePath, StandardCopyOption.REPLACE_EXISTING);
                            }
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    });
                } else {
                    if (!checkAndUpdateMD5ChecksumRegistry(invalidateAllHashes, hashRegistry, envFile, touched)) {
                        System.out.println("- " + getRelativePath(envFile));
                        updated.set(true);
                        Files.copy(envFile.toPath(), fs.getPath("/python_resources/" + envFile.getName()), StandardCopyOption.REPLACE_EXISTING);
                    }

                }
            }
            // Now delete removed files
            // Delete untouched
            Set<String> toRemove = new HashSet<>();
            hashRegistry.fieldNames().forEachRemaining(entry -> {
                if (!touched.contains(entry)) {
                    System.out.println("- " + entry);
                    toRemove.add(entry);
                }
            });
            hashRegistry.remove(toRemove);
            if (toRemove.size() > 0) {
                updated.set(true);
                toRemove.forEach(f -> {
                    try {
                        Files.deleteIfExists(fs.getPath(f));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return updated.get();
    }

    private static String getRelativePath(File f) {
        File root = new File(".");
        return root.toURI().relativize(f.toURI()).getPath();
    }
}
