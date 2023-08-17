package org.ohnlp.backbone.api.config.xlang;

import org.ohnlp.backbone.api.ComponentLang;
import org.ohnlp.backbone.api.config.BackbonePipelineComponentConfiguration;

public class PythonBackbonePipelineComponentConfiguration extends BackbonePipelineComponentConfiguration {
    String bundleName;
    String entryPoint;
    String entryClass;

    public PythonBackbonePipelineComponentConfiguration() {
        setLang(ComponentLang.PYTHON);
    }

    public String getBundleName() {
        return bundleName;
    }

    public void setBundleName(String bundleName) {
        this.bundleName = bundleName;
    }

    public String getEntryPoint() {
        return entryPoint;
    }

    public void setEntryPoint(String entryPoint) {
        this.entryPoint = entryPoint;
    }

    public String getEntryClass() {
        return entryClass;
    }

    public void setEntryClass(String entryClass) {
        this.entryClass = entryClass;
    }
}
