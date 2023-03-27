package org.ohnlp.backbone.configurator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.ohnlp.backbone.api.BackbonePipelineComponent;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

public class Main {
    public static void main(String... args) throws IOException {
        ObjectMapper om = new ObjectMapper();
        List<ModuleInfo> modules = new ArrayList<>();
        // Iterate through modules
        for (File f : Objects.requireNonNull(new File("modules").listFiles())) {
            JarFile jar = new JarFile(f);
            // First, load module in
            ZipEntry entry = jar.getEntry("backbone_module.json");
            if (entry != null) {
                modules.add(om.readValue(jar.getInputStream(entry), ModuleInfo.class));
            }
            jar.close();

        }
//        // Scan classpath for annotated entities
//        System.out.println("Scanning classpath for components. This may take a while..");
//        ClassPathScanningCandidateComponentProvider scanner
//                = new ClassPathScanningCandidateComponentProvider(true);
//        scanner.addIncludeFilter(new AssignableTypeFilter(BackbonePipelineComponent.class));
//        Set<BeanDefinition> components = scanner.findCandidateComponents("");
//        components.forEach(component -> {
//            System.out.println(component.getBeanClassName());
//
//        });
//        System.out.println("Component scan complete!");

    }


}
