package com.example.pipeline.config;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;

public class TransformationConfig implements Serializable {
    private String type;
    private Map<String, Object> properties;
    private String condition;
    private String expression;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getProperties() {
        return properties != null ? properties : new HashMap<>();
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }
} 