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
import java.util.HashMap;
import java.util.Map;

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
            String condition = config.getProperties().get("condition").toString();
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
            String prefix = config.getProperties().getOrDefault("prefix", "").toString();
            String suffix = config.getProperties().getOrDefault("suffix", "").toString();
            return prefix + item + suffix;
        }
    }

    private StreamStage<String> applyTransformations(StreamStage<String> stage) {
        if (config.getTransformations() == null) {
            return stage;
        }

        StreamStage<String> current = stage;
        for (TransformationConfig transformation : config.getTransformations()) {
            current = switch (transformation.getType().toLowerCase()) {
                case "filter" -> current.filter(new SerializableFilter(transformation));
                case "map" -> current.map(new SerializableMapper(transformation));
                default -> {
                    logger.warn("Unknown transformation type: {}", transformation.getType());
                    yield current;
                }
            };
        }
        return current;
    }

    private StreamStage<String> applyTransformation(StreamStage<String> stage, TransformationConfig config) {
        return switch (config.getType().toLowerCase()) {
            case "filter" -> applyFilter(stage, config);
            case "map" -> applyMap(stage, config);
            default -> throw new IllegalArgumentException("Unknown transformation type: " + config.getType());
        };
    }

    private StreamStage<String> applyFilter(StreamStage<String> stage, TransformationConfig config) {
        Map<String, Object> props = config.getProperties();
        String condition = props.getOrDefault("condition", "").toString();
        String column = props.getOrDefault("column", "").toString();

        if (column != null && !column.isEmpty()) {
            // CSV filtering
            return stage.filter(line -> {
                String[] parts = line.split("\\|");
                if (parts.length >= 3 && parts[1].equals("TYPE=CSV")) {
                    String[] fields = parts[2].split(",");
                    // TODO: Add proper CSV parsing with header mapping
                    return fields.length > 2 && fields[2].contains(condition);
                }
                return line.contains(condition);
            });
        } else {
            // Text filtering
            return stage.filter(line -> line.contains(condition));
        }
    }

    private StreamStage<String> applyMap(StreamStage<String> stage, TransformationConfig config) {
        Map<String, Object> props = config.getProperties();
        String format = props.getOrDefault("format", "text").toString();

        if ("csv".equals(format)) {
            @SuppressWarnings("unchecked")
            Map<String, String> columnMapping = (Map<String, String>) props.getOrDefault("columnMapping", new HashMap<>());
            return stage.map(line -> mapCsvLine(line, columnMapping));
        } else {
            String prefix = props.getOrDefault("prefix", "").toString();
            String suffix = props.getOrDefault("suffix", "").toString();
            return stage.map(line -> prefix + line + suffix);
        }
    }

    private String mapCsvLine(String line, Map<String, String> columnMapping) {
        String[] parts = line.split("\\|");
        if (parts.length >= 3 && parts[1].equals("TYPE=CSV")) {
            // Process CSV content
            String[] fields = parts[2].split(",");
            // TODO: Implement proper CSV field mapping
            return parts[0] + "|" + parts[1] + "|" + parts[2];
        }
        return line;
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