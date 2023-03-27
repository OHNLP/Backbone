package org.ohnlp.backbone.configurator;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.schemas.Schema;
import org.ohnlp.backbone.api.BackbonePipelineComponent;
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.configurator.structs.ModuleConfigField;
import org.ohnlp.backbone.configurator.structs.ModuleInfo;
import org.ohnlp.backbone.configurator.structs.types.*;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.type.filter.AssignableTypeFilter;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;

public class Main {
    public static void main(String... args) throws IOException {
        File[] files = Objects.requireNonNull(new File("modules").listFiles());
        // Set up a global classloader with visibility in modules folder
        URLClassLoader cl = new URLClassLoader(Arrays.stream(files).map(f -> {
            try {
                return f.toURI().toURL();
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }).toArray(URL[]::new));
        // Iterate through modules to load module info
        Map<ModuleDeclaration, List<ModuleInfo>> modules = scanForModules(files, cl);
        return;
    }

    private static Map<ModuleDeclaration, List<ModuleInfo>> scanForModules(File[] files, URLClassLoader cl) throws IOException {
        ObjectMapper om = new ObjectMapper();
        HashMap<ModuleDeclaration, List<ModuleInfo>> ret = new HashMap<>();
        System.out.println("Scanning for Modules...");
        for (File f : files) {
            JarFile jar = new JarFile(f);
            ZipEntry entry = jar.getEntry("backbone_module.json");
            if (entry == null) {
                continue;
            }
            ModuleDeclaration dec = om.readValue(jar.getInputStream(entry), ModuleDeclaration.class);
            ret.put(dec, new ArrayList<>());
            jar.close();
            ClassPathScanningCandidateComponentProvider provider
                    = new ClassPathScanningCandidateComponentProvider(true);
            provider.setResourceLoader(new DefaultResourceLoader(cl));
            provider.addIncludeFilter(new AssignableTypeFilter(BackbonePipelineComponent.class));
            // Scan packages for annotated entities
            if (dec.getPackages() != null) {
                new HashSet<>(dec.getPackages()).forEach(
                        basePackage -> {
                            Set<BeanDefinition> components = provider.findCandidateComponents(basePackage);
                            components.forEach(component -> {
                                try {
                                    Class<? extends BackbonePipelineComponent<?,?>> clazz
                                            = (Class<? extends BackbonePipelineComponent<?,?>>) Class.forName(component.getBeanClassName(), true, cl);
                                    if (!Modifier.isAbstract(clazz.getModifiers())) {
                                        ModuleInfo module = new ModuleInfo(clazz);
                                        loadModuleInfo(module, om);
                                        ret.get(dec).add(module);
                                    }
                                } catch (ClassNotFoundException e) {
                                    throw new RuntimeException(e);
                                }
                            });
                        }
                );
            }
        }
        System.out.println("Module Scan Complete!");
        return ret;
    }

    /**
     * Scan the provided module's annotations and populate relevant fields in the module
     * @param module The module to load
     */
    private static void loadModuleInfo(ModuleInfo module, ObjectMapper om) {
        Class<? extends BackbonePipelineComponent<?, ?>> clazz = module.getClazz();
        ComponentDescription desc = clazz.getDeclaredAnnotation(ComponentDescription.class);
        module.setName(desc.name());
        module.setDesc(desc.desc());
        module.setRequires(desc.requires());
        Arrays.stream(clazz.getDeclaredFields()).forEachOrdered(f -> {
            if (f.isAnnotationPresent(ConfigurationProperty.class)) {
                ModuleConfigField field = loadModuleConfigField(om, f);
                module.getConfigFields().add(field);
            }
        });
    }

    private static ModuleConfigField loadModuleConfigField(ObjectMapper om, Field f) {
        ModuleConfigField ret = new ModuleConfigField();
        ConfigurationProperty config = f.getDeclaredAnnotation(ConfigurationProperty.class);
        ret.setPath(config.path());
        ret.setDesc(config.desc());
        ret.setRequired(config.required());
        JavaType javaType = om.constructType(f.getGenericType());
        if (config.isInputColumn()) {
            ret.setImpl(new InputColumnTypedConfigurationField(javaType.isArrayType() || javaType.isCollectionLikeType()));
        } else {
            ret.setImpl(resolveTypedConfig(javaType));
        }
        return ret;
    }

    private static TypedConfigurationField resolveTypedConfig(JavaType t) {
        if (t.isArrayType() || t.isCollectionLikeType()) {
            return resolveTypedConfigCollection(t);
        } else if (t.isPrimitive()) {
            return resolveTypedConfigPrimitive(t);
        } else if (t.isTypeOrSubTypeOf(String.class)) {
            return new StringTypedConfigurationField();
        } else if (t.isTypeOrSubTypeOf(Boolean.class)) {
            return new BooleanTypedConfigurationField();
        } else if (t.isTypeOrSubTypeOf(Number.class)) {
            return resolveTypedConfigNumber(t);
        } else if (t.isEnumImplType()) {
            return resolveTypedConfigEnum(t);
        } else if (t.isMapLikeType()) {
            return resolveTypedConfigMap(t);
        } else if (t.isTypeOrSubTypeOf(Schema.class)) {
            return new SchemaTypedConfigurationField();
        } else if (!t.isConcrete()) {
            throw new IllegalArgumentException("Abstract classes and interfaces cannot be used for configuration parameters");
        } else {
            return resolveTypedConfigPOJO(t);
        }
    }

    private static CollectionTypedConfigurationField resolveTypedConfigCollection(JavaType t) {
        CollectionTypedConfigurationField ret = new CollectionTypedConfigurationField();
        ret.setContents(resolveTypedConfig(t.getContentType()));
        return ret;
    }

    private static TypedConfigurationField resolveTypedConfigPrimitive(JavaType t) {
        Class<?> clz = t.getRawClass();
        if (clz.equals(boolean.class)) {
            return new BooleanTypedConfigurationField();
        } else if (clz.equals(char.class)) {
            return new CharacterTypedConfigurationField();
        } else if (clz.equals(byte.class)) {
            return new NumericTypedConfigurationField(false, Byte.MIN_VALUE, Byte.MAX_VALUE);
        } else if (clz.equals(short.class)) {
            return new NumericTypedConfigurationField(false, Short.MIN_VALUE, Short.MAX_VALUE);
        } else if (clz.equals(int.class)) {
            return new NumericTypedConfigurationField(false, Integer.MIN_VALUE, Integer.MAX_VALUE);
        } else if (clz.equals(long.class)) {
            return new NumericTypedConfigurationField(false, Long.MIN_VALUE, Long.MAX_VALUE);
        } else if (clz.equals(float.class)) {
            return new NumericTypedConfigurationField(true, Float.MIN_VALUE, Float.MAX_VALUE);
        } else if (clz.equals(double.class)) {
            return new NumericTypedConfigurationField(true, Double.MIN_VALUE, Double.MAX_VALUE);
        } else {
            throw new IllegalArgumentException("Primitive is of type void");
        }
    }

    private static TypedConfigurationField resolveTypedConfigNumber(JavaType t) {
        // TODO We cannot exactly process all possible implementations of Number, so default to floating/double and just
        // error out at runtime if incorrect
        return new NumericTypedConfigurationField(true, Double.MIN_VALUE, Double.MAX_VALUE);
    }

    private static TypedConfigurationField resolveTypedConfigEnum(JavaType t) {
        return new EnumerationTypedConfigurationField(
                Arrays.stream(((Class<? extends Enum>) t.getRawClass()).getEnumConstants())
                .map(f -> f.name())
                .collect(Collectors.toList())
        );
    }

    private static TypedConfigurationField resolveTypedConfigMap(JavaType t) {
        TypedConfigurationField key = resolveTypedConfig(t.getKeyType());
        TypedConfigurationField value = resolveTypedConfig(t.getContentType());
        return new MapTypedConfigurationField(key, value);
    }

    private static TypedConfigurationField resolveTypedConfigPOJO(JavaType t) {
        Map<String, TypedConfigurationField> fields = new HashMap<>();
        ObjectMapper om = new ObjectMapper();
        for (Field f : t.getRawClass().getDeclaredFields()) {
            fields.put(f.getName(), resolveTypedConfig(om.constructType(f.getGenericType())));
        }
        return new ObjectTypedConfigurationField(fields);
    }
}
