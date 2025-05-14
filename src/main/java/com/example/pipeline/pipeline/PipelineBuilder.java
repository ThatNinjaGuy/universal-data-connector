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
import java.util.ArrayList;
import java.util.List;

public class PipelineBuilder {
    private static final Logger logger = LoggerFactory.getLogger(PipelineBuilder.class);
    private final PipelineConfig.Pipeline config;

    public PipelineBuilder(PipelineConfig.Pipeline config) {
        this.config = config;
        validateConfig(config);
    }

    private static class SerializableFilter implements Serializable, PredicateEx<String> {
        private final String condition;
        private final String column;

        SerializableFilter(TransformationConfig config) {
            Map<String, Object> props = config.getProperties();
            this.condition = props.getOrDefault("condition", "").toString();
            this.column = props.getOrDefault("column", "").toString();
            logger.debug("Created filter with condition: {} for column: {}", condition, column);
        }

        @Override
        public boolean testEx(String item) {
            try {
                if (column != null && !column.isEmpty()) {
                    // CSV filtering
                    String[] parts = item.split("\\|", -1);
                    if (parts.length >= 3 && parts[1].equals("TYPE=CSV")) {
                        String csvContent = parts[2];
                        String[] lines = csvContent.split("\n", -1);
                        
                        if (lines.length < 2) {
                            logger.warn("CSV content has insufficient lines: {}", lines.length);
                            return false;
                        }
                        
                        // Get header and find column index
                        String[] headers = lines[0].split(",");
                        int columnIndex = -1;
                        for (int i = 0; i < headers.length; i++) {
                            if (headers[i].trim().equalsIgnoreCase(column)) {
                                columnIndex = i;
                                break;
                            }
                        }
                        
                        if (columnIndex == -1) {
                            logger.warn("Column {} not found in headers: {}", column, String.join(",", headers));
                            return false;
                        }
                        
                        // Check each data row
                        for (int i = 1; i < lines.length; i++) {
                            String[] fields = lines[i].split(",", -1);
                            if (fields.length > columnIndex) {
                                String value = fields[columnIndex].trim();
                                if (value.contains(condition)) {
                                    logger.debug("Found matching condition '{}' in column {} with value: {}", 
                                        condition, column, value);
                                    return true;
                                }
                            }
                        }
                        return false;
                    }
                }
                return item.contains(condition);
            } catch (Exception e) {
                logger.error("Error in filter: {}", e.getMessage());
                return false;
            }
        }
    }

    private static class SerializableMapper implements Serializable, FunctionEx<String, String> {
        private final String format;
        private final String prefix;
        private final String suffix;
        private final Map<String, String> columnMapping;

        SerializableMapper(TransformationConfig config) {
            Map<String, Object> props = config.getProperties();
            this.format = props.getOrDefault("format", "text").toString();
            this.prefix = props.getOrDefault("prefix", "").toString();
            this.suffix = props.getOrDefault("suffix", "").toString();
            @SuppressWarnings("unchecked")
            Map<String, String> mapping = (Map<String, String>) props.getOrDefault("columnMapping", new HashMap<>());
            this.columnMapping = mapping;
            logger.debug("Created mapper with format: {}, columnMapping: {}", format, columnMapping);
        }

        @Override
        public String applyEx(String item) {
            try {
                if ("csv".equals(format)) {
                    return mapCsvLine(item);
                } else {
                    return prefix + item + suffix;
                }
            } catch (Exception e) {
                logger.error("Error in mapper: {}", e.getMessage());
                return item;
            }
        }

        private String mapCsvLine(String line) {
            String[] parts = line.split("\\|", -1);
            if (parts.length >= 3 && parts[1].equals("TYPE=CSV")) {
                String sourceFile = parts[0];
                String csvContent = parts[2];
                String[] lines = csvContent.split("\n", -1);
                
                if (lines.length < 2) {
                    logger.warn("CSV content has insufficient lines: {}", lines.length);
                    return line;
                }
                
                // Process headers
                String[] headers = lines[0].split(",", -1);
                Map<Integer, Integer> columnIndexMap = new HashMap<>();
                List<String> newHeaders = new ArrayList<>();
                
                // Create mapping from source columns to target columns
                for (Map.Entry<String, String> entry : columnMapping.entrySet()) {
                    String targetColumn = entry.getKey();
                    String sourceColumn = entry.getValue();
                    
                    // Find source column index
                    for (int i = 0; i < headers.length; i++) {
                        if (headers[i].trim().equalsIgnoreCase(sourceColumn)) {
                            columnIndexMap.put(i, newHeaders.size());
                            newHeaders.add(targetColumn);
                            break;
                        }
                    }
                }
                
                if (columnIndexMap.isEmpty()) {
                    logger.warn("No matching columns found in mapping");
                    return line;
                }
                
                // Build new CSV content
                StringBuilder newContent = new StringBuilder();
                
                // Add new headers
                newContent.append(String.join(",", newHeaders)).append("\n");
                
                // Process data rows
                for (int i = 1; i < lines.length; i++) {
                    String[] fields = lines[i].split(",", -1);
                    String[] newFields = new String[newHeaders.size()];
                    
                    // Map fields to new positions
                    for (Map.Entry<Integer, Integer> entry : columnIndexMap.entrySet()) {
                        int sourceIndex = entry.getKey();
                        int targetIndex = entry.getValue();
                        
                        if (sourceIndex < fields.length) {
                            newFields[targetIndex] = fields[sourceIndex].trim();
                        } else {
                            newFields[targetIndex] = "";
                        }
                    }
                    
                    newContent.append(String.join(",", newFields)).append("\n");
                }
                
                return sourceFile + "|TYPE=CSV|" + newContent.toString();
            }
            return line;
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

    public Pipeline build() {
        try {
            logger.info("Building pipeline '{}' with source: {} and sink: {}", 
                config.getName(), config.getSource().getType(), config.getSink().getType());

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
    
    private static void validateConfig(PipelineConfig.Pipeline config) {
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