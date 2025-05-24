package com.example.pipeline.factory;

import com.example.pipeline.config.SinkConfig;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.function.FunctionEx;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import com.example.pipeline.factory.sink.ParquetSinkContext;
import com.example.pipeline.factory.sink.FileSinkContext;
import com.example.pipeline.factory.sink.S3SinkContext;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

public class SinkFactory {
    private static final Logger logger = LoggerFactory.getLogger(SinkFactory.class);

    private static class SerializableKafkaConfig implements Serializable {
        private final Properties props;
        private final String topic;

        SerializableKafkaConfig(Map<String, String> properties) {
            this.props = new Properties();
            
            // Set bootstrap servers with correct property name
            this.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get("bootstrapServers"));
            this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            
            this.topic = properties.get("topic");
            
            // Add debug logging
            logger.info("Created Kafka config with properties: {}", this.props);
        }

        Properties getProperties() {
            return props;
        }

        String getTopic() {
            return topic;
        }
    }

    private static class KafkaSinkContext implements Serializable {
        private final SerializableKafkaConfig config;
        private transient KafkaProducer<String, String> producer;

        KafkaSinkContext(SerializableKafkaConfig config) {
            this.config = config;
        }

        void init() {
            if (producer == null) {
                producer = new KafkaProducer<>(config.props);
            }
        }

        void receive(String item) {
            try {
                producer.send(new ProducerRecord<>(config.topic, item, item));
            } catch (Exception e) {
                logger.warn("Error sending to Kafka: {}", e.getMessage());
            }
        }

        void close() {
            if (producer != null) {
                producer.close();
                producer = null;
            }
        }
    }

    private static class SerializableContextFactory implements Serializable, FunctionEx<Processor.Context, KafkaSinkContext> {
        private final SerializableKafkaConfig config;

        SerializableContextFactory(SerializableKafkaConfig config) {
            this.config = config;
        }

        @Override
        public KafkaSinkContext applyEx(Processor.Context context) {
            KafkaSinkContext kafkaContext = new KafkaSinkContext(config);
            kafkaContext.init();
            return kafkaContext;
        }
    }

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
        SerializableKafkaConfig kafkaConfig = new SerializableKafkaConfig(config.getProperties());
        SerializableContextFactory contextFactory = new SerializableContextFactory(kafkaConfig);
        
        return SinkBuilder
            .sinkBuilder("kafka-sink", contextFactory)
            .<String>receiveFn(KafkaSinkContext::receive)
            .destroyFn(KafkaSinkContext::close)
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
            .sinkBuilder("file-sink", ctx -> new FileSinkContext(directory, extension, includeHeaders))
            .<String>receiveFn(FileSinkContext::write)
            .destroyFn(FileSinkContext::close)
            .build();
    }

    private static Sink<String> createParquetSink(Map<String, String> props) {
        validateParquetConfig(props);
        
        String directory = props.get("path");
        String schema = props.get("schema");
        int batchSize = Integer.parseInt(props.getOrDefault("batchSize", "1000"));
        
        return SinkBuilder
            .sinkBuilder("parquet-sink", ctx -> new ParquetSinkContext(directory, schema, batchSize))
            .<String>receiveFn((context, item) -> {
                if (item != null && !item.isEmpty()) {
                    logger.info("Received item for Parquet processing: {}", item);
                    
                    // Handle both SOURCE= and JDBC_SOURCE prefixes
                    if (!item.startsWith("SOURCE=") && !item.startsWith("JDBC_SOURCE")) {
                        logger.error("Invalid item format for Parquet processing: {}", item);
                        return;
                    }
                    
                    logger.debug("Writing item to Parquet: {}", item);
                    ((ParquetSinkContext)context).write(item);
                }
            })
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

    private static void validateParquetConfig(Map<String, String> props) {
        if (!props.containsKey("path")) {
            throw new IllegalArgumentException("Parquet sink requires 'path' property");
        }
        if (!props.containsKey("schema")) {
            throw new IllegalArgumentException("Parquet sink requires 'schema' property");
        }
        try {
            // Validate schema format
            String schema = props.get("schema");
            // Remove whitespace and newlines for validation
            String normalizedSchema = schema.replaceAll("\\s+", "");
            
            // Basic Avro schema validation
            if (!normalizedSchema.contains("\"type\":\"record\"")) {
                throw new IllegalArgumentException("Invalid Avro schema format: missing type record");
            }
            if (!normalizedSchema.contains("\"fields\"")) {
                throw new IllegalArgumentException("Invalid Avro schema format: missing fields");
            }
            if (!normalizedSchema.contains("\"name\"")) {
                throw new IllegalArgumentException("Invalid Avro schema format: missing name");
            }
            
            // Log the schema for debugging
            logger.debug("Validated Parquet schema: {}", schema);
        } catch (Exception e) {
            logger.error("Schema validation failed: {}", e.getMessage());
            throw new IllegalArgumentException("Invalid Parquet schema: " + e.getMessage());
        }
    }

    private static Sink<String> createJdbcSink(SinkConfig config) {
        Map<String, String> props = config.getProperties();
        String query = props.get("query");
        logger.info("Creating JDBC sink with query: {}", query);
        
        return Sinks.jdbc(
            query,
            props.get("jdbcUrl"),
            (stmt, item) -> {
                try {
                    String[] parts = item.split("\\|", -1);
                    if (parts.length >= 3 && parts[1].equals("TYPE=CSV")) {
                        String csvContent = parts[2];
                        String[] lines = csvContent.split("\n", -1);
                        
                        if (lines.length > 1) {
                            // Process all rows except header
                            for (int rowNum = 1; rowNum < lines.length; rowNum++) {
                                String line = lines[rowNum].trim();
                                if (!line.isEmpty()) {
                                    String[] values = line.split(",", -1);
                                    logger.debug("Processing row {}: {}", rowNum, line);
                                    
                                    // Map values to prepared statement parameters
                                    stmt.setInt(1, Integer.parseInt(values[0].trim())); // id
                                    stmt.setString(2, values[1].trim()); // name
                                    stmt.setString(3, values[2].trim()); // email
                                    stmt.setString(4, values[3].trim()); // department
                                    stmt.setDouble(5, Double.parseDouble(values[4].trim())); // salary
                                    
                                    stmt.addBatch();
                                    logger.debug("Added row to batch: {}", line);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error preparing JDBC statement: {}", e.getMessage());
                    throw new RuntimeException("Failed to prepare JDBC statement", e);
                }
            }
        );
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
            .sinkBuilder("s3-sink", ctx -> new S3SinkContext(props))
            .<String>receiveFn((context, item) -> {
                if (item != null && !item.isEmpty()) {
                    logger.debug("Received item for S3 upload: {}", item);
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