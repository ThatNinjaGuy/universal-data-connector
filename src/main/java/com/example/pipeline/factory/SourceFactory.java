package com.example.pipeline.factory;

import com.example.pipeline.config.SourceConfig;
import com.example.pipeline.factory.source.JdbcSourceContext;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.BatchSource;
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
import java.sql.SQLException;
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

    /**
     * Context class for file-based source operations. Handles continuous monitoring of a directory
     * for new files matching a specified pattern. Files are processed once and moved to a processed
     * directory to prevent duplicate processing.
     */
    private static class FileSourceContext implements Serializable {
        private final String directory;
        private final Pattern filePattern;
        // Static set to maintain processed files across context instances
        // This ensures files are only processed once even if the context is recreated
        private static final Set<String> processedFiles = new HashSet<>();
        private static final Logger logger = LoggerFactory.getLogger(FileSourceContext.class);

        /**
         * Initializes the file source context with a directory to monitor and a file pattern to match.
         * The pattern supports glob-style wildcards (e.g., "*.csv", "data_*.txt").
         *
         * @param directory The directory path to monitor for new files
         * @param pattern The glob pattern to match files against
         */
        FileSourceContext(String directory, String pattern) {
            this.directory = directory;
            this.filePattern = convertGlobToRegex(pattern);
            logger.info("Initialized FileSourceContext with directory: {} and pattern: {}", directory, pattern);
        }

        /**
         * Converts a glob pattern to a regular expression pattern.
         * Supports common glob wildcards: * (any sequence), ? (single character)
         *
         * @param glob The glob pattern to convert
         * @return A compiled Pattern object for matching filenames
         */
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

        /**
         * Checks if a filename matches the configured pattern.
         *
         * @param filename The filename to check
         * @return true if the filename matches the pattern, false otherwise
         */
        private boolean matchesPattern(String filename) {
            return filePattern.matcher(filename).matches();
        }

        /**
         * Detects the type of file based on its extension and content.
         * Currently supports CSV files (based on extension and header presence)
         * and defaults to TEXT for all other files.
         *
         * @param filename The name of the file
         * @param content The content of the file
         * @return The detected file type ("CSV" or "TEXT")
         */
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

        /**
         * Reads and processes new files from the monitored directory.
         * This method:
         * 1. Scans the directory for files matching the pattern
         * 2. Skips already processed files
         * 3. Reads and processes new files
         * 4. Moves processed files to a 'processed' subdirectory
         * 5. Returns the processed content with metadata
         *
         * @return List of processed file contents with metadata
         */
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

                // List files matching the pattern
                File[] files = dir.listFiles((d, name) -> {
                    File f = new File(d, name);
                    boolean matches = f.isFile() && matchesPattern(name);
                    logger.debug("File {} matches pattern: {}", name, matches);
                    return matches;
                });
                
                if (files != null) {
                    for (File file : files) {
                        String fileName = file.getName();
                        // Skip if file has already been processed
                        if (!processedFiles.contains(fileName)) {
                            try {
                                // Read entire file content at once
                                String content = Files.readString(file.toPath());
                                
                                // Detect file type
                                String fileType = detectFileType(fileName, content);
                                logger.info("Processing file {} of type {}", fileName, fileType);
                                
                                // Create metadata string with source file info
                                // Format: SOURCE=<filename>|TYPE=<filetype>|<content>
                                String item = String.format("SOURCE=%s|TYPE=%s|%s",
                                    fileName,
                                    fileType,
                                    content);
                                items.add(item);
                                
                                // Move file to processed directory to prevent reprocessing
                                File processedDir = new File("data/processed");
                                if (!processedDir.exists()) {
                                    processedDir.mkdirs();
                                }
                                
                                File destFile = new File(processedDir, fileName);
                                if (file.renameTo(destFile)) {
                                    processedFiles.add(fileName);
                                    logger.info("Successfully processed and moved {} to processed directory", fileName);
                                } else {
                                    logger.error("Failed to move file {} to processed directory", fileName);
                                }
                            } catch (IOException e) {
                                logger.error("Failed to read file {}: {}", fileName, e.getMessage());
                            }
                        } else {
                            logger.debug("File {} has already been processed, skipping", fileName);
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
                case "jdbc" -> createJdbcSource(config); 
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