package org.ohnlp.backbone.api.config.xlang;

import org.ohnlp.backbone.api.config.BackbonePipelineComponentConfiguration;

public class PythonBackbonePipelineComponentConfiguration extends BackbonePipelineComponentConfiguration {
    String bundleName;
    String envName;
    String entryPoint;
    String entryClass;

    public String getBundleName() {
        return bundleName;
    }

    public void setBundleName(String bundleName) {
        this.bundleName = bundleName;
    }

    public String getEnvName() {
        return envName;
    }

    public void setEnvName(String envName) {
        this.envName = envName;
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
