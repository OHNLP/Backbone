package org.ohnlp.backbone.configurator.structs;

import org.ohnlp.backbone.api.BackbonePipelineComponent;

import java.util.ArrayList;
import java.util.List;

public class ModuleInfo {
    private String name = "";
    private String desc = "";
    private String[] requires = {};
    private Class<? extends BackbonePipelineComponent<?,?>> clazz;
    private final List<ModuleConfigField> configFields= new ArrayList<>();

    public ModuleInfo(Class<? extends BackbonePipelineComponent<?,?>> clazz) {
        this.clazz = clazz;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String[] getRequires() {
        return requires;
    }

    public void setRequires(String[] requires) {
        this.requires = requires;
    }

    public Class<? extends BackbonePipelineComponent<?,?>> getClazz() {
        return clazz;
    }

    public void setClazz(Class<? extends BackbonePipelineComponent<?,?>> clazz) {
        this.clazz = clazz;
    }

    public List<ModuleConfigField> getConfigFields() {
        return configFields;
    }
}
