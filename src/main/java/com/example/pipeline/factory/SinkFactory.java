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
        String prefix = props.getOrDefault("prefix", "output");
        String extension = props.getOrDefault("extension", ".txt");

        // Create processed directory if it doesn't exist
        new File("data/processed").mkdirs();

        return SinkBuilder
            .sinkBuilder("file-sink", ctx -> new FileSinkContext(directory, prefix, extension))
            .<String>receiveFn((FileSinkContext context, String item) -> {
                // Parse metadata to get source file info
                String[] parts = item.split("\\|", 2);
                String sourceFile = parts[0].substring("SOURCE_FILE:".length());
                String content = parts[1].substring("CONTENT:".length());
                
                // Create unique output file name for this source file
                String timestamp = String.format("%tY%<tm%<td_%<tH%<tM%<tS", new Date());
                String outputFile = String.format("%s/%s_%s_%s%s",
                    directory, prefix, 
                    sourceFile.replace(".txt", ""),
                    timestamp, extension);
                
                context.write(outputFile, content + "\n");
            })
            .destroyFn(FileSinkContext::close)
            .build();
    }

    private static Sink<String> createJdbcSink(SinkConfig config) {
        String query = "INSERT INTO " + config.getProperties().get("table") + " (value) VALUES (?)";
        return Sinks.jdbc(
            query,
            config.getProperties().get("jdbcUrl"),
            (stmt, item) -> stmt.setString(1, item)
        );
    }

    private static class FileSinkContext implements Serializable {
        private final String directory;
        private final String prefix;
        private final String extension;
        private final Map<String, BufferedWriter> writers = new HashMap<>();
        private final Set<String> processedFiles = new HashSet<>();

        FileSinkContext(String directory, String prefix, String extension) {
            this.directory = directory;
            this.prefix = prefix;
            this.extension = extension;
        }

        void write(String filename, String item) {
            try {
                BufferedWriter writer = writers.computeIfAbsent(filename, this::createWriter);
                writer.write(item);
                writer.flush();
            } catch (IOException e) {
                logger.error("Failed to write to file: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        }

        private BufferedWriter createWriter(String filename) {
            try {
                return new BufferedWriter(new FileWriter(filename, true));
            } catch (IOException e) {
                logger.error("Failed to create writer for file: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        }

        void close() {
            writers.values().forEach(writer -> {
                try {
                    writer.close();
                } catch (IOException e) {
                    logger.error("Failed to close writer: {}", e.getMessage());
                }
            });
            writers.clear();
        }

        String getSourceFile(String item) {
            // This should be enhanced to get actual source file info from your pipeline
            return "data/input/test.txt"; // Placeholder - needs actual implementation
        }

        void moveToProcessed(String sourceFile) {
            if (!processedFiles.contains(sourceFile)) {
                try {
                    File source = new File(sourceFile);
                    File dest = new File("data/processed/" + source.getName());
                    if (source.renameTo(dest)) {
                        processedFiles.add(sourceFile);
                        logger.info("Moved {} to processed directory", source.getName());
                    }
                } catch (Exception e) {
                    logger.error("Failed to move file: {}", e.getMessage());
                }
            }
        }
    }
} 