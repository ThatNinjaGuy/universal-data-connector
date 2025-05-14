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
        
        String prefix = props.getOrDefault("prefix", "output");
        String extension = props.getOrDefault("extension", ".txt");
        boolean includeHeaders = Boolean.parseBoolean(props.getOrDefault("includeHeaders", "true"));

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
        private final boolean includeHeaders;
        private final Map<String, BufferedWriter> writers = new HashMap<>();
        private final Set<String> processedFiles = new HashSet<>();

        FileSinkContext(String directory, String prefix, String extension, boolean includeHeaders) {
            this.directory = directory;
            this.prefix = prefix;
            this.extension = extension;
            this.includeHeaders = includeHeaders;
        }

        void write(String item) {
            try {
                String[] parts = item.split("\\|", -1);
                if (parts.length < 3) {
                    logger.error("Invalid item format: {}", item);
                    return;
                }
                
                String sourceFile = parts[0].substring("SOURCE=".length());
                String fileType = parts[1].substring("TYPE=".length());
                String content = parts[2];
                
                // Create output filename
                String timestamp = String.format("%tY%<tm%<td_%<tH%<tM%<tS", new Date());
                String outputFile = String.format("%s/%s_%s_%s%s",
                    directory, prefix, 
                    sourceFile.replace(fileType.equals("CSV") ? ".csv" : ".txt", ""),
                    timestamp,
                    extension);
                
                logger.info("Processing {} file: {} -> {}", fileType, sourceFile, outputFile);
                
                if (fileType.equals("TEXT")) {
                    // Text file processing
                    content = "processed-" + content + "-done";
                    BufferedWriter writer = writers.computeIfAbsent(outputFile, this::createWriter);
                    writer.write(content + "\n");
                    writer.flush();
                } else if (fileType.equals("CSV")) {
                    // CSV file processing
                    String[] lines = content.split("\n", -1);
                    if (lines.length > 0) {
                        StringBuilder processedContent = new StringBuilder();
                        
                        // Handle headers
                        if (includeHeaders && lines.length > 0) {
                            processedContent.append(lines[0]).append("\n");
                        }
                        
                        // Process data rows
                        for (int i = 1; i < lines.length; i++) {
                            String line = lines[i].trim();
                            if (!line.isEmpty()) {
                                processedContent.append(line).append("\n");
                            }
                        }
                        
                        BufferedWriter writer = writers.computeIfAbsent(outputFile, this::createWriter);
                        writer.write(processedContent.toString());
                        writer.flush();
                        logger.info("Wrote processed CSV content to: {}", outputFile);
                    }
                }
            } catch (IOException e) {
                logger.error("Error processing item: {}", e.getMessage());
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