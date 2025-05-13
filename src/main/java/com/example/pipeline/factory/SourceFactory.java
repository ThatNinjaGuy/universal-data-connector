package com.example.pipeline.factory;

import com.example.pipeline.config.SourceConfig;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.function.FunctionEx;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class SourceFactory {
    private static final Logger logger = LoggerFactory.getLogger(SourceFactory.class);

    private static class SerializableKafkaConfig implements Serializable {
        private final Properties props;
        private final String topic;

        SerializableKafkaConfig(Map<String, String> properties) {
            this.props = new Properties();
            
            this.props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get("bootstrapServers"));
            this.props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.get("groupId"));
            this.props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.get("autoOffsetReset"));
            this.props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            this.props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            
            this.topic = properties.get("topic");
            
            logger.info("Created Kafka config with properties: {}", this.props);
        }

        Properties getProperties() {
            return props;
        }

        String getTopic() {
            return topic;
        }
    }

    private static class KafkaSourceContext implements Serializable {
        private final SerializableKafkaConfig config;
        private KafkaConsumer<String, String> consumer;

        KafkaSourceContext(SerializableKafkaConfig config) {
            this.config = config;
        }

        void init() {
            Properties props = new Properties();
            props.putAll(config.getProperties());
            
            logger.info("Initializing Kafka consumer with properties: {}", props);
            
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(config.getTopic()));
            
            logger.info("Successfully created Kafka consumer for topic: {}", config.getTopic());
        }

        void fillBuffer(SourceBuilder.SourceBuffer<String> buffer) {
            try {
                for (ConsumerRecord<String, String> record : consumer.poll(java.time.Duration.ofMillis(100))) {
                    buffer.add(record.value());
                }
            } catch (Exception e) {
                logger.warn("Error polling Kafka: {}", e.getMessage());
            }
        }

        void close() {
            if (consumer != null) {
                consumer.close();
                consumer = null;
            }
        }
    }

    private static class SerializableContextFactory implements Serializable, FunctionEx<Processor.Context, KafkaSourceContext> {
        private final SerializableKafkaConfig config;

        SerializableContextFactory(SerializableKafkaConfig config) {
            this.config = config;
        }

        @Override
        public KafkaSourceContext applyEx(Processor.Context context) {
            KafkaSourceContext kafkaContext = new KafkaSourceContext(config);
            kafkaContext.init();
            return kafkaContext;
        }
    }

    private static class FileSourceContext implements Serializable {
        // Empty context class - processing moved to fillBufferFn
    }

    public static StreamSource<String> create(SourceConfig config) {
        if (config == null || config.getType() == null) {
            throw new IllegalArgumentException("Source configuration is missing or invalid");
        }

        try {
            return switch (config.getType().toLowerCase()) {
                case "kafka" -> createKafkaSource(config);
                case "file" -> createFileSource(config);
                default -> throw new IllegalArgumentException("Unknown source type: " + config.getType());
            };
        } catch (Exception e) {
            throw new RuntimeException("Failed to create source: " + e.getMessage(), e);
        }
    }

    private static StreamSource<String> createKafkaSource(SourceConfig config) {
        validateKafkaConfig(config);
        SerializableKafkaConfig kafkaConfig = new SerializableKafkaConfig(config.getProperties());
        SerializableContextFactory contextFactory = new SerializableContextFactory(kafkaConfig);
        
        return SourceBuilder
            .stream("kafka-source", contextFactory)
            .fillBufferFn(KafkaSourceContext::fillBuffer)
            .destroyFn(KafkaSourceContext::close)
            .build();
    }

    private static StreamSource<String> createFileSource(SourceConfig config) {
        validateFileConfig(config);
        String path = config.getProperties().get("path");
        
        return SourceBuilder
            .stream("file-source", ctx -> new FileSourceContext())
            .<String>fillBufferFn((context, buffer) -> {
                try (var dirStream = Files.list(Paths.get(path))) {
                    dirStream
                        .filter(Files::isRegularFile)
                        .forEach(file -> processFile(file, buffer));
                } catch (IOException e) {
                    logger.error("Failed to list files: {}", e.getMessage());
                }
                // Add delay to prevent continuous scanning
                Thread.sleep(1000);
            })
            .build();
    }

    private static void processFile(Path file, SourceBuilder.SourceBuffer<String> buffer) {
        // Skip already processed files
        if (file.getFileName().toString().startsWith(".") || 
            Files.exists(Paths.get("data/processed", file.getFileName().toString()))) {
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            String line;
            String fileName = file.getFileName().toString();
            boolean isCsv = fileName.endsWith(".csv");
            
            // Add metadata with file type
            String metadataPrefix = String.format("SOURCE=%s|TYPE=%s|", 
                fileName, 
                isCsv ? "CSV" : "TEXT");

            while ((line = reader.readLine()) != null) {
                buffer.add(metadataPrefix + line);
            }
            
            // Move file after processing
            Path processedDir = Paths.get("data/processed");
            if (!Files.exists(processedDir)) {
                Files.createDirectories(processedDir);
            }
            Files.move(file, processedDir.resolve(fileName));
            logger.info("Processed and moved file: {}", fileName);
        } catch (IOException e) {
            logger.error("Failed to process file {}: {}", file, e.getMessage());
        }
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
} 