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
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.util.Set;
import java.util.regex.Pattern;

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
        private final String directory;
        private final Pattern filePattern;
        private final Set<String> processedFiles = new HashSet<>();

        FileSourceContext(String directory, String pattern) {
            this.directory = directory;
            this.filePattern = convertGlobToRegex(pattern);
            logger.info("Initialized FileSourceContext with directory: {} and pattern: {}", directory, pattern);
        }

        private static Pattern convertGlobToRegex(String glob) {
            if (glob == null || glob.isEmpty()) {
                return Pattern.compile(".*");
            }
            StringBuilder regex = new StringBuilder();
            regex.append("^");
            for (int i = 0; i < glob.length(); i++) {
                char c = glob.charAt(i);
                switch (c) {
                    case '*' -> regex.append(".*");
                    case '?' -> regex.append(".");
                    case '.' -> regex.append("\\.");
                    case '\\' -> {
                        regex.append("\\\\");
                        if (i + 1 < glob.length()) {
                            regex.append(glob.charAt(++i));
                        }
                    }
                    default -> regex.append(c);
                }
            }
            regex.append("$");
            return Pattern.compile(regex.toString(), Pattern.CASE_INSENSITIVE);
        }

        private boolean matchesPattern(String filename) {
            return filePattern.matcher(filename).matches();
        }

        private String detectFileType(String filename, String content) {
            if (filename.toLowerCase().endsWith(".csv")) {
                // Additional CSV validation
                String[] lines = content.split("\n", 2);
                if (lines.length > 0 && lines[0].contains(",")) {
                    return "CSV";
                }
            }
            return "TEXT";
        }

        List<String> readNewFiles() {
            List<String> items = new ArrayList<>();
            try {
                File dir = new File(directory);
                if (!dir.exists()) {
                    dir.mkdirs();
                    logger.info("Created directory: {}", directory);
                    return items;
                }

                if (!dir.isDirectory()) {
                    logger.error("Path is not a directory: {}", directory);
                    return items;
                }

                File[] files = dir.listFiles((d, name) -> {
                    File f = new File(d, name);
                    boolean matches = f.isFile() && matchesPattern(name);
                    logger.debug("File {} matches pattern: {}", name, matches);
                    return matches;
                });
                
                if (files != null) {
                    for (File file : files) {
                        if (!processedFiles.contains(file.getName())) {
                            try {
                                // Read entire file content at once
                                String content = Files.readString(file.toPath());
                                
                                // Detect file type
                                String fileType = detectFileType(file.getName(), content);
                                logger.info("Detected file type {} for file {}", fileType, file.getName());
                                
                                // Create metadata string with source file info
                                String item = String.format("SOURCE=%s|TYPE=%s|%s",
                                    file.getName(),
                                    fileType,
                                    content);
                                items.add(item);
                                
                                // Move file to processed directory
                                File processedDir = new File("data/processed");
                                if (!processedDir.exists()) {
                                    processedDir.mkdirs();
                                }
                                
                                File destFile = new File(processedDir, file.getName());
                                if (file.renameTo(destFile)) {
                                    processedFiles.add(file.getName());
                                    logger.info("Moved {} to processed directory", file.getName());
                                } else {
                                    logger.error("Failed to move file {} to processed directory", file.getName());
                                }
                            } catch (IOException e) {
                                logger.error("Failed to read file {}: {}", file.getName(), e.getMessage());
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to read files: {}", e.getMessage(), e);
            }
            return items;
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
        String pattern = config.getProperties().getOrDefault("pattern", ".*");
        
        return SourceBuilder
            .stream("file-source", ctx -> new FileSourceContext(path, pattern))
            .<String>fillBufferFn((context, buffer) -> {
                List<String> items = ((FileSourceContext) context).readNewFiles();
                items.forEach(buffer::add);
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
} 