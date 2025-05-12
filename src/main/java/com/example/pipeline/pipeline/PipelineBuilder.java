package com.example.pipeline.pipeline;

import com.example.pipeline.config.PipelineConfig;
import com.example.pipeline.config.TransformationConfig;
import com.example.pipeline.factory.SinkFactory;
import com.example.pipeline.factory.SourceFactory;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class PipelineBuilder {
    private static final Logger logger = LoggerFactory.getLogger(PipelineBuilder.class);
    private final PipelineConfig config;

    public PipelineBuilder(PipelineConfig config) {
        this.config = config;
        validateConfig(config);
    }

    private static class SerializableFilter implements Serializable, PredicateEx<String> {
        private final TransformationConfig config;

        SerializableFilter(TransformationConfig config) {
            this.config = config;
        }

        @Override
        public boolean testEx(String item) {
            String condition = config.getProperties().get("condition");
            return item != null && item.contains(condition);
        }
    }

    private static class SerializableMapper implements Serializable, FunctionEx<String, String> {
        private final TransformationConfig config;

        SerializableMapper(TransformationConfig config) {
            this.config = config;
        }

        @Override
        public String applyEx(String item) {
            String prefix = config.getProperties().getOrDefault("prefix", "");
            String suffix = config.getProperties().getOrDefault("suffix", "");
            return prefix + item + suffix;
        }
    }

    private StreamStage<String> applyTransformations(StreamStage<String> stage) {
        if (config.getTransformations() == null) {
            return stage;
        }

        StreamStage<String> current = stage;
        for (TransformationConfig transformation : config.getTransformations()) {
            switch (transformation.getType().toLowerCase()) {
                case "filter" -> current = current.filter(new SerializableFilter(transformation));
                case "map" -> current = current.map(new SerializableMapper(transformation));
                default -> logger.warn("Unknown transformation type: {}", transformation.getType());
            }
        }
        return current;
    }

    public Pipeline build() {
        try {
            logger.info("Building pipeline with source: {} and sink: {}", 
                config.getSource().getType(), config.getSink().getType());

            Pipeline pipeline = Pipeline.create();
            StreamStage<String> source = pipeline
                .readFrom(SourceFactory.create(config.getSource()))
                .withoutTimestamps();

            StreamStage<String> transformed = applyTransformations(source);

            transformed.writeTo(SinkFactory.create(config.getSink()));

            return pipeline;
        } catch (Exception e) {
            throw new RuntimeException("Failed to build pipeline", e);
        }
    }
    
    private static void validateConfig(PipelineConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("Pipeline configuration cannot be null");
        }
        if (config.getSource() == null) {
            throw new IllegalArgumentException("Source configuration is required");
        }
        if (config.getSink() == null) {
            throw new IllegalArgumentException("Sink configuration is required");
        }
    }
} 