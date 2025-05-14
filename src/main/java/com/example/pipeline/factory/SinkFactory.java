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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
import java.util.HashMap;
import java.io.File;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

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
        String prefix = props.getOrDefault("prefix", "output");
        String extension = props.getOrDefault("extension", ".txt");
        boolean includeHeaders = Boolean.parseBoolean(props.getOrDefault("includeHeaders", "true"));

        if ("parquet".equals(format)) {
            String schema = props.get("schema");
            return SinkBuilder
                .sinkBuilder("parquet-sink", (ctx) -> new ParquetSinkContext(directory, schema))
                .<String>receiveFn((context, item) -> {
                    String[] parts = item.split("\\|", -1);
                    if (parts.length >= 3 && parts[1].equals("TYPE=CSV")) {
                        String sourceFile = parts[0].substring("SOURCE=".length());
                        String csvContent = parts[2];
                        
                        logger.info("Processing CSV file: {} with content:\n{}", sourceFile, csvContent);
                        
                        Map<String, Integer> headerMap = extractHeaders(csvContent);
                        ((ParquetSinkContext)context).write(csvContent, headerMap, sourceFile);
                    }
                })
                .destroyFn(context -> ((ParquetSinkContext)context).close())
                .build();
        }

        return SinkBuilder
            .sinkBuilder("file-sink", ctx -> new FileSinkContext(directory, prefix, extension, includeHeaders))
            .<String>receiveFn(FileSinkContext::write)
            .destroyFn(FileSinkContext::close)
            .build();
    }

    private static Map<String, Integer> extractHeaders(String csvContent) {
        Map<String, Integer> headerMap = new HashMap<>();
        String[] lines = csvContent.split("\n", -1);
        if (lines.length > 0) {
            String headerLine = lines[0];
            logger.info("Found header line: {}", headerLine);
            String[] headers = headerLine.split(",");
            
            for (int i = 0; i < headers.length; i++) {
                String header = headers[i].trim();
                headerMap.put(header, i);
                logger.info("Mapped header '{}' to column {}", header, i);
            }
        }
        return headerMap;
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

    private static class FileSinkContext implements Serializable {
        private final String directory;
        private final String prefix;
        private final String extension;
        private final boolean includeHeaders;
        private BufferedWriter writer;
        private boolean isFirstWrite = true;

        FileSinkContext(String directory, String prefix, String extension, boolean includeHeaders) {
            this.directory = directory;
            this.prefix = prefix;
            this.extension = extension;
            this.includeHeaders = includeHeaders;
            
            // Create output directory if it doesn't exist
            File dir = new File(directory);
            if (!dir.exists()) {
                dir.mkdirs();
            }
        }

        void write(String item) {
            try {
                if (writer == null) {
                    String timestamp = String.format("%1$tY%1$tm%1$td_%1$tH%1$tM%1$tS", new Date());
                    String filename = String.format("%s_%s%s", prefix, timestamp, extension);
                    File outputFile = new File(directory, filename);
                    writer = new BufferedWriter(new FileWriter(outputFile));
                    logger.info("Created new CSV file: {}", outputFile.getAbsolutePath());
                }

                writer.write(item);
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                logger.error("Failed to write to file: {}", e.getMessage());
                throw new RuntimeException("Failed to write to file", e);
            }
        }

        void close() {
            try {
                if (writer != null) {
                    writer.close();
                    writer = null;
                }
            } catch (IOException e) {
                logger.error("Failed to close writer: {}", e.getMessage());
            }
        }
    }
} 