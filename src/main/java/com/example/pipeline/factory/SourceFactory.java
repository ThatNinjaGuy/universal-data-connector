package com.example.pipeline.factory;

import com.example.pipeline.config.SourceConfig;
import com.example.pipeline.factory.source.JdbcSourceContext;
import com.example.pipeline.factory.source.FileSourceContext;
import com.example.pipeline.factory.source.KafkaSourceContext;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.function.FunctionEx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Map;
import java.util.List;

public class SourceFactory {
    private static final Logger logger = LoggerFactory.getLogger(SourceFactory.class);

    private static class SerializableContextFactory implements Serializable, FunctionEx<Processor.Context, KafkaSourceContext> {
        private final Map<String, String> properties;

        SerializableContextFactory(Map<String, String> properties) {
            this.properties = properties;
        }

        @Override
        public KafkaSourceContext applyEx(Processor.Context context) {
            KafkaSourceContext kafkaContext = new KafkaSourceContext(properties);
            kafkaContext.init();
            return kafkaContext;
        }
    }

    public static StreamSource<String> create(SourceConfig config) {
        if (config == null || config.getType() == null) {
            throw new IllegalArgumentException("Source configuration is missing or invalid");
        }

        try {
            return switch (config.getType().toLowerCase()) {
                case "kafka" -> createKafkaSource(config);
                case "file" -> createFileSource(config);
                case "jdbc" -> createJdbcSource(config); 
                default -> throw new IllegalArgumentException("Unknown source type: " + config.getType());
            };
        } catch (Exception e) {
            throw new RuntimeException("Failed to create source: " + e.getMessage(), e);
        }
    }

    private static StreamSource<String> createKafkaSource(SourceConfig config) {
        validateKafkaConfig(config);
        SerializableContextFactory contextFactory = new SerializableContextFactory(config.getProperties());
        
        return SourceBuilder
            .stream("kafka-source", contextFactory)
            .fillBufferFn(KafkaSourceContext::fillBuffer)
            .destroyFn(KafkaSourceContext::close)
            .build();
    }

    private static StreamSource<String> createFileSource(SourceConfig config) {
        validateFileConfig(config);
        String path = config.getProperties().get("path");
        String pattern = config.getProperties().getOrDefault("pattern", ".*");
        
        return SourceBuilder
            .stream("file-source", ctx -> new FileSourceContext(path, pattern))
            .<String>fillBufferFn((context, buffer) -> {
                List<String> items = ((FileSourceContext) context).readNewFiles();
                items.forEach(buffer::add);
            })
            .build();
    }

    private static StreamSource<String> createJdbcSource(SourceConfig config) {
        validateJdbcConfig(config);
        Map<String, String> properties = config.getProperties();
        
        return SourceBuilder
            .stream("jdbc-source", ctx -> new JdbcSourceContext(properties))
            .<String>fillBufferFn((context, buffer) -> {
                try {
                    List<String> batch = ((JdbcSourceContext) context).readBatch();
                    if (!batch.isEmpty()) {
                        batch.forEach(buffer::add);
                    } else {
                        // If no more records and it's a one-time operation, sleep to avoid busy polling
                        if (Boolean.parseBoolean(properties.getOrDefault("oneTimeOperation", "false"))) {
                            Thread.sleep(Long.MAX_VALUE);
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to read from database: " + e.getMessage(), e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            })
            .destroyFn(context -> {
                try {
                    ((JdbcSourceContext) context).close();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to close JDBC source: " + e.getMessage(), e);
                }
            })
            .build();
    }

    private static void validateKafkaConfig(SourceConfig config) {
        if (!config.getProperties().containsKey("bootstrapServers")) {
            throw new IllegalArgumentException("Kafka source requires 'bootstrapServers' property");
        }
        if (!config.getProperties().containsKey("topic")) {
            throw new IllegalArgumentException("Kafka source requires 'topic' property");
        }
    }

    private static void validateFileConfig(SourceConfig config) {
        if (!config.getProperties().containsKey("path")) {
            throw new IllegalArgumentException("File source requires 'path' property");
        }
    }

    private static void validateJdbcConfig(SourceConfig config) {
        if (!config.getProperties().containsKey("jdbcUrl")) {
            throw new IllegalArgumentException("JDBC source requires 'jdbcUrl' property");
        }
        if (!config.getProperties().containsKey("user")) {
            throw new IllegalArgumentException("JDBC source requires 'user' property");
        }
        if (!config.getProperties().containsKey("table") && !config.getProperties().containsKey("query")) {
            throw new IllegalArgumentException("JDBC source requires either 'table' or 'query' property");
        }
    }
} 