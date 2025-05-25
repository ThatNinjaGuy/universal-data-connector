package com.example.pipeline.factory;

import com.example.pipeline.config.SinkConfig;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.pipeline.factory.sink.ParquetSinkContext;
import com.example.pipeline.factory.sink.FileSinkContext;
import com.example.pipeline.factory.sink.S3SinkContext;
import com.example.pipeline.factory.sink.JdbcSinkContext;
import com.example.pipeline.factory.sink.KafkaSinkContext;

import java.util.Map;

public class SinkFactory {
    private static final Logger logger = LoggerFactory.getLogger(SinkFactory.class);

    public static Sink<String> create(SinkConfig config) {
        if (config == null || config.getType() == null) {
            throw new IllegalArgumentException("Sink configuration is missing or invalid");
        }

        try {
            return switch (config.getType().toLowerCase()) {
                case "kafka" -> createKafkaSink(config);
                case "file" -> createFileSink(config);
                case "jdbc" -> createJdbcSink(config);
                case "s3" -> createS3Sink(config);
                default -> throw new IllegalArgumentException("Unknown sink type: " + config.getType());
            };
        } catch (Exception e) {
            throw new RuntimeException("Failed to create sink: " + e.getMessage(), e);
        }
    }

    private static Sink<String> createKafkaSink(SinkConfig config) {
        Map<String, String> props = config.getProperties();
        logger.info("Creating Kafka sink for topic: {}", props.get("topic"));
        
        return SinkBuilder
            .sinkBuilder("kafka-sink", ctx -> {
                KafkaSinkContext context = new KafkaSinkContext(props);
                context.init();
                return context;
            })
            .<String>receiveFn(KafkaSinkContext::receive)
            .destroyFn(context -> {
                try {
                    logger.info("Destroying KafkaSinkContext");
                    ((KafkaSinkContext)context).close();
                    logger.info("Successfully destroyed KafkaSinkContext");
                } catch (Exception e) {
                    logger.error("Error destroying KafkaSinkContext: {}", e.getMessage(), e);
                    throw e;
                }
            })
            .build();
    }

    private static Sink<String> createFileSink(SinkConfig config) {
        Map<String, String> props = config.getProperties();
        String directory = props.get("path");
        String format = props.getOrDefault("format", "text");
        String extension = props.getOrDefault("extension", ".txt");
        boolean includeHeaders = Boolean.parseBoolean(props.getOrDefault("includeHeaders", "true"));

        if ("parquet".equals(format)) {
            return createParquetSink(props);
        }

        return SinkBuilder
            .sinkBuilder("file-sink", ctx -> {
                FileSinkContext context = new FileSinkContext(directory, extension, includeHeaders);
                context.init();
                return context;
            })
            .<String>receiveFn(FileSinkContext::receive)
            .destroyFn(FileSinkContext::close)
            .build();
    }

    private static Sink<String> createParquetSink(Map<String, String> props) {
        ParquetSinkContext.validateConfig(props);
        
        String directory = props.get("path");
        String schema = props.get("schema");
        int batchSize = Integer.parseInt(props.getOrDefault("batchSize", "1000"));
        
        return SinkBuilder
            .sinkBuilder("parquet-sink", ctx -> {
                ParquetSinkContext context = new ParquetSinkContext(directory, schema, batchSize);
                context.init();
                return context;
            })
            .<String>receiveFn(ParquetSinkContext::receive)
            .destroyFn(context -> {
                try {
                    logger.info("Destroying ParquetSinkContext, closing all writers");
                    ((ParquetSinkContext)context).close();
                    logger.info("Successfully destroyed ParquetSinkContext");
                } catch (Exception e) {
                    logger.error("Error destroying ParquetSinkContext: {}", e.getMessage(), e);
                    throw e;
                }
            })
            .build();
    }

    private static Sink<String> createJdbcSink(SinkConfig config) {
        Map<String, String> props = config.getProperties();
        String query = props.get("query");
        logger.info("Creating JDBC sink with query: {}", query);
        
        return SinkBuilder
            .sinkBuilder("jdbc-sink", ctx -> {
                JdbcSinkContext context = new JdbcSinkContext(props);
                context.init();
                return context;
            })
            .<String>receiveFn(JdbcSinkContext::receive)
            .destroyFn(context -> {
                try {
                    logger.info("Destroying JdbcSinkContext");
                    ((JdbcSinkContext)context).close();
                    logger.info("Successfully destroyed JdbcSinkContext");
                } catch (Exception e) {
                    logger.error("Error destroying JdbcSinkContext: {}", e.getMessage(), e);
                    throw e;
                }
            })
            .build();
    }

    private static Sink<String> createS3Sink(SinkConfig config) {
        Map<String, String> props = config.getProperties();
        
        // Validate required properties
        if (!props.containsKey("bucketName")) {
            throw new IllegalArgumentException("S3 sink requires 'bucketName' property");
        }
        if (!props.containsKey("region")) {
            throw new IllegalArgumentException("S3 sink requires 'region' property");
        }
        if (!props.containsKey("accessKey")) {
            throw new IllegalArgumentException("S3 sink requires 'accessKey' property");
        }
        if (!props.containsKey("secretKey")) {
            throw new IllegalArgumentException("S3 sink requires 'secretKey' property");
        }

        logger.info("Creating S3 sink for bucket: {}", props.get("bucketName"));
        
        return SinkBuilder
            .sinkBuilder("s3-sink", ctx -> {
                S3SinkContext context = new S3SinkContext(props);
                context.init(); // Initialize the context
                return context;
            })
            .<String>receiveFn((context, item) -> {
                if (item != null && !item.isEmpty()) {
                    ((S3SinkContext)context).receive(item);
                }
            })
            .destroyFn(context -> {
                try {
                    logger.info("Destroying S3SinkContext");
                    ((S3SinkContext)context).close();
                    logger.info("Successfully destroyed S3SinkContext");
                } catch (Exception e) {
                    logger.error("Error destroying S3SinkContext: {}", e.getMessage(), e);
                    throw e;
                }
            })
            .build();
    }
} 