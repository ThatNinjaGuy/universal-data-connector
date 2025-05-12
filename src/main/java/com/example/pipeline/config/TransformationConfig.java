package com.example.pipeline.config;

import java.io.Serializable;
import java.util.Map;

public class TransformationConfig implements Serializable {
    private String type;
    private Map<String, String> properties;
    private String condition;
    private String expression;

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