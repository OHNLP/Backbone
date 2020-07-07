package org.ohnlp.backbone.pluginmanager;

import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.FileNameUtils;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class PluginManager {

    public static void main(String... args) throws IOException {
        List<File> modules = Arrays.asList(Objects.requireNonNull(new File("modules").listFiles()));
        List<File> configs = Arrays.asList(Objects.requireNonNull(new File("configs").listFiles()));
        List<File> resources = Arrays.asList(Objects.requireNonNull(new File("resources").listFiles()));
        File source = new File("bin/Backbone-Core.jar");
        File target = new File("bin/Backbone-Core-Packaged.jar");
        Files.copy(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
        install(target, modules, configs, resources);
        JOptionPane.showMessageDialog(null, "Packaging complete! Final Packaged File: " + target.getAbsolutePath(), "Backbone Packaging Complete", JOptionPane.ERROR_MESSAGE);
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
        try (FileSystem fs = FileSystems.newFileSystem(target.toURI(), env)) {
            for (File module : modules) {
                Files.copy(module.toPath(), fs.getPath("/lib/" + module.getName()), StandardCopyOption.REPLACE_EXISTING);
            }
            for (File config : configurations) {
                Files.copy(config.toPath(), fs.getPath("/configs/" + config.getName()), StandardCopyOption.REPLACE_EXISTING);
            }
            for (File resource : resources) {
                String resourceDir = resource.getParentFile().toPath().toAbsolutePath().toString();
                if (resource.isDirectory()) {
                    // Recursively find all files in directory (up to arbitrary max depth of 999
                    Files.find(resource.toPath(), 999, (p, bfa) -> bfa.isRegularFile()).forEach(p -> {
                        try {
                            Files.copy(p, fs.getPath(p.toAbsolutePath().toString().replace(resourceDir, "/resources/")), StandardCopyOption.REPLACE_EXISTING);
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
