package com.example.pipeline.config;

import java.io.Serializable;
import java.util.List;

public class PipelineConfig implements Serializable {
    private SourceConfig source;
    private SinkConfig sink;
    private List<TransformationConfig> transformations;

    // Getters and Setters
    public SourceConfig getSource() {
        return source;
    }

    public void setSource(SourceConfig source) {
        this.source = source;
    }

    public SinkConfig getSink() {
        return sink;
    }

    public void setSink(SinkConfig sink) {
        this.sink = sink;
    }

    public List<TransformationConfig> getTransformations() {
        return transformations;
    }

    public void setTransformations(List<TransformationConfig> transformations) {
        this.transformations = transformations;
    }
} 