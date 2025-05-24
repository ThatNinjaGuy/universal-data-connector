package com.example.pipeline.config;

import java.io.Serializable;
import java.util.Map;

public class SourceConfig implements Serializable {
    private String type;
    private Map<String, String> properties;

    // Getters and Setters
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
} 