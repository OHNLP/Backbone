package org.ohnlp.backbone.pluginmanager;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;

public class PluginManager {

    public static void main(String... args) throws IOException {
        Set<String> builds = new HashSet<>();
        if (args.length > 0) {
            builds = Arrays.stream(args).map(String::toLowerCase).collect(Collectors.toSet());
        }
        List<File> modules = Arrays.asList(Objects.requireNonNull(new File("modules").listFiles()));
        List<File> configs = Arrays.asList(Objects.requireNonNull(new File("configs").listFiles()));
        List<File> resources = Arrays.asList(Objects.requireNonNull(new File("resources").listFiles()));
        for (File f : new File("bin").listFiles()) {
            if (!f.isDirectory()) {
                if (f.getName().startsWith("Backbone-Core") && !f.getName().endsWith("Packaged.jar")) {
                    File source = f;
                    String type = f.getName().substring(14, f.getName().length() - 4);
                    if (builds.size() == 0 || builds.contains(type.toLowerCase(Locale.ROOT))) {
                        File target = new File("bin/Backbone-Core-" + type + "-Packaged.jar");
                        Files.copy(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
                        install(target, modules, configs, resources);
                        System.out.println("Successfully Packaged Platform-Specific JAR: " + target.getAbsolutePath());
                    }
                }
            }
        }
        System.out.println("Packaging complete!");
    }

    /**
     * Packs an OHNLP backbone executable with the specified set of modules,  configurations, and resources
     *
     * @param target         The target file for packaging/into which items should be installed
     * @param modules        A list of module jar files to install
     * @param configurations A set of configuration files to install
     * @param resources      A set of resource directories to install
     */
    public static void install(File target, List<File> modules, List<File> configurations, List<File> resources) {
        Map<String, String> env = new HashMap<>();
        env.put("create", "false");
        try (FileSystem fs = FileSystems.newFileSystem(target.toPath(), PluginManager.class.getClassLoader())) {
            for (File module : modules) {
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
            Files.createDirectories(fs.getPath("/configs"));
            for (File config : configurations) {
                Files.copy(config.toPath(), fs.getPath("/configs/" + config.getName()), StandardCopyOption.REPLACE_EXISTING);
            }
            Files.createDirectories(fs.getPath("/resources"));
            for (File resource : resources) {
                String resourceDir = resource.getParentFile().toPath().toAbsolutePath().toString();
                if (resource.isDirectory()) {
                    // Recursively find all files in directory (up to arbitrary max depth of 999
                    Files.find(resource.toPath(), 999, (p, bfa) -> bfa.isRegularFile()).forEach(p -> {
                        try {
                            Path filePath = fs.getPath(p.toAbsolutePath().toString().replace(resourceDir, "/resources"));
                            Files.createDirectories(filePath.getParent());
                            Files.copy(p, filePath, StandardCopyOption.REPLACE_EXISTING);
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    });
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
