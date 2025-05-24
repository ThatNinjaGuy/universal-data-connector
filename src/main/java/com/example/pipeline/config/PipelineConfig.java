package com.example.pipeline.config;

import java.io.Serializable;
import java.util.List;

public class PipelineConfig implements Serializable {
    private List<Pipeline> pipelines;

    public static class Pipeline implements Serializable {
        private String name;
        private SourceConfig source;
        private SinkConfig sink;
        private List<TransformationConfig> transformations;

        // Getters and Setters
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

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

    // Getters and Setters
    public List<Pipeline> getPipelines() {
        return pipelines;
    }

    public void setPipelines(List<Pipeline> pipelines) {
        this.pipelines = pipelines;
    }
} 