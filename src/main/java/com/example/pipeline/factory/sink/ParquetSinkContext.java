package com.example.pipeline.factory.sink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ParquetSinkContext implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ParquetSinkContext.class);
    private final String directory;
    private final String schema;
    private final Map<String, ParquetWriter<GenericRecord>> writers = new HashMap<>();
    private transient Schema avroSchema;
    private Map<String, Integer> columnMapping;
    private final int batchSize;
    private final ConcurrentMap<String, List<GenericRecord>> batchMap = new ConcurrentHashMap<>();
    
    public ParquetSinkContext(String directory, String schema, int batchSize) {
        this.directory = directory;
        this.schema = schema;
        this.batchSize = batchSize;
        try {
            this.avroSchema = new Schema.Parser().parse(schema);
            logger.info("Initialized ParquetSinkContext with directory: {}, schema: {}, batch size: {}", 
                directory, avroSchema.getName(), batchSize);
            
            // Create output directory if it doesn't exist
            File outDir = new File(directory);
            if (!outDir.exists()) {
                outDir.mkdirs();
                logger.info("Created output directory: {}", directory);
            }
        } catch (Exception e) {
            logger.error("Failed to parse Avro schema: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to parse Avro schema", e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Initiating graceful shutdown...");
            close();
        }));
    }

    public void write(String item) {
        try {
            if (item.startsWith("JDBC_SOURCE")) {
                processJdbcFormattedItem(item);
            } 
            else if (item.startsWith("SOURCE=")) {
                processFileBasedItem(item);
            } 
            else {
                logger.error("Unrecognized item format: {}", item);
            }
        } catch (Exception e) {
            logger.error("Error processing item: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process item", e);
        }
    }

    private void processFileBasedItem(String item) {
        String[] parts = item.split("\\|", -1);
        if (parts.length < 3) {
            logger.error("Invalid item format: {}", item);
            return;
        }

        String sourceFile = parts[0].substring("SOURCE=".length());
        String fileType = parts[1].substring("TYPE=".length());
        String content = parts[2].trim();

        logger.debug("Processing file-based item - sourceFile: {}, fileType: {}, content length: {}", 
            sourceFile, fileType, content.length());

        if (!fileType.equals("CSV")) {
            logger.warn("Unsupported file type: {}, skipping", fileType);
            return;
        }

        try {
            processCSVContent(sourceFile, content);
        } catch (Exception e) {
            throw new RuntimeException("Failed to process file content", e);
        }
    }

    private void processJdbcFormattedItem(String item) {
        String[] parts = item.split("\\|", -1);
        if (parts.length < 3 || !parts[0].equals("JDBC_SOURCE")) {
            logger.error("Invalid JDBC item format: {}", item);
            return;
        }

        String content = parts[2];
        try {
            // Ensure writer exists before processing
            getWriter("jdbc_export"); 
            processCSVContent("jdbc_export", content);
        } catch (Exception e) {
            throw new RuntimeException("Failed to process JDBC content", e);
        }
    }

    private void processCSVContent(String sourceFile, String content) {
        String[] lines = content.split("\n", -1);
        if (lines.length == 0) {
            logger.warn("Empty CSV content, skipping");
            return;
        }

        // Process header for column mapping if needed
        if (columnMapping == null) {
            initializeColumnMapping(lines[0]);
            if (lines.length == 1) return; // Only header present
        }

        // Process data rows
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i].trim();
            if (!line.isEmpty()) {
                String[] values = line.split(",", -1);
                GenericRecord record = createRecordFromValues(values);
                if (record != null) {
                    try {
                        addToBatch(sourceFile, record);
                    } catch (Exception e) {
                        throw new RuntimeException("Batch processing failed", e);
                    }
                }
            }
        }
        
        // Force flush after processing each file
        try {
            flushBatch(sourceFile);
            // Close the writer after flushing to ensure content is written
            closeWriter(sourceFile);
        } catch (Exception e) {
            logger.error("Failed to flush batch for {}: {}", sourceFile, e.getMessage());
            throw new RuntimeException("Failed to flush batch", e);
        }
    }

    private void initializeColumnMapping(String headerLine) {
        String[] headers = headerLine.split(",", -1);
        columnMapping = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            String header = headers[i].trim();
            // Remove quotes if present
            if (header.startsWith("\"") && header.endsWith("\"")) {
                header = header.substring(1, header.length() - 1);
            }
            // Map headers to schema fields (case-insensitive)
            for (Schema.Field field : avroSchema.getFields()) {
                if (field.name().equalsIgnoreCase(header)) {
                    columnMapping.put(field.name(), i);
                    logger.info("Mapped schema field '{}' to column {} (header: {})", field.name(), i, header);
                    break;
                }
            }
        }
        logger.info("Initialized column mapping: {}", columnMapping);
    }

    private GenericRecord createRecordFromValues(String[] values) {
        if (values.length < columnMapping.size()) {
            logger.warn("Row has fewer columns than expected: {}", String.join(",", values));
            return null;
        }

        GenericRecord record = new GenericData.Record(avroSchema);
        boolean hasData = false;
        StringBuilder fieldDebug = new StringBuilder();

        for (Schema.Field field : avroSchema.getFields()) {
            String fieldName = field.name();
            Integer columnIndex = columnMapping.get(fieldName);

            if (columnIndex != null && columnIndex < values.length) {
                String value = values[columnIndex].trim();
                // Remove quotes if present
                if (value.startsWith("\"") && value.endsWith("\"")) {
                    value = value.substring(1, value.length() - 1).replace("\"\"", "\"");
                }

                Object convertedValue = convertToAvroType(value, field.schema());
                if (convertedValue != null) {
                    hasData = true;
                    record.put(fieldName, convertedValue);
                    fieldDebug.append(fieldName).append("='").append(value).append("', ");
                } else {
                    record.put(fieldName, getDefaultValue(field.schema()));
                }
            } else {
                logger.warn("No mapping found for field {} or index {} out of bounds (values length: {})", 
                    fieldName, columnIndex, values.length);
                record.put(fieldName, getDefaultValue(field.schema()));
            }
        }

        if (hasData) {
            logger.debug("Created record with fields: {}", fieldDebug);
            return record;
        } else {
            logger.debug("Skipping record as it contains no data");
            return null;
        }
    }

    private void addToBatch(String writerKey, GenericRecord record) throws Exception {
        List<GenericRecord> batch = batchMap.computeIfAbsent(writerKey, k -> new ArrayList<>());
        batch.add(record);
        
        if (batch.size() >= batchSize) {
            logger.info("Batch size reached {} for {}, flushing...", batchSize, writerKey);
            flushBatch(writerKey);
        }
    }

    private void flushBatch(String writerKey) throws Exception {
        List<GenericRecord> batch = batchMap.remove(writerKey);
        if (batch != null && !batch.isEmpty()) {
            // Get or create writer for this batch
            ParquetWriter<GenericRecord> writer = getWriter(writerKey);

            try {
                for (GenericRecord record : batch) {
                    writer.write(record);
                }
                logger.info("Flushed batch of {} records for {}", batch.size(), writerKey);
            } catch (IOException e) {
                logger.error("Batch write failed", e);
                throw new RuntimeException("Failed to write batch", e);
            }
        }
    }

    private void closeWriter(String writerKey) {
        ParquetWriter<GenericRecord> writer = writers.remove(writerKey);
        if (writer != null) {
            try {
                writer.close();
                logger.info("Closed writer for {}", writerKey);
            } catch (IOException e) {
                logger.error("Failed to close writer for {}: {}", writerKey, e.getMessage());
            }
        }
    }

    private ParquetWriter<GenericRecord> createWriter(String sourceFile) {
        try {
            String timestamp = String.format("%tY%<tm%<td_%<tH%<tM%<tS", new Date());
            String fileName = sourceFile + "_" + timestamp + ".parquet";
            Path path = new Path(new File(directory, fileName).getAbsolutePath());
            
            logger.info("Creating new Parquet writer for file: {}", path);
            
            ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
                .withSchema(avroSchema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withDictionaryEncoding(true)
                .withValidation(true)
                .withConf(new Configuration())
                .build();

            logger.info("Successfully created Parquet writer for file: {}", path);
            return writer;
        } catch (Exception e) {
            logger.error("Failed to create Parquet writer: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to create Parquet writer", e);
        }
    }

    private Object convertToAvroType(String value, Schema schema) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }

        Schema.Type type = schema.getType();
        if (type == Schema.Type.UNION) {
            // For union types, find the non-null type
            for (Schema s : schema.getTypes()) {
                if (s.getType() != Schema.Type.NULL) {
                    return convertToAvroType(value, s);
                }
            }
            return null;
        }
        
        try {
            value = value.trim();
            return switch (type) {
                case STRING -> value;
                case INT -> {
                    // Handle PostgreSQL int4 and remove any decimal points
                    if (value.contains(".")) {
                        yield Integer.parseInt(value.substring(0, value.indexOf(".")));
                    }
                    yield Integer.parseInt(value);
                }
                case LONG -> Long.parseLong(value);
                case FLOAT -> Float.parseFloat(value);
                case DOUBLE -> {
                    // Remove currency symbols and commas
                    String cleanValue = value.replaceAll("[^\\d.]", "");
                    yield Double.parseDouble(cleanValue);
                }
                case BOOLEAN -> Boolean.parseBoolean(value);
                default -> {
                    logger.warn("Unsupported Avro type: {}", type);
                    yield null;
                }
            };
        } catch (Exception e) {
            logger.error("Failed to convert value '{}' to type {}: {}", value, type, e.getMessage());
            return null;
        }
    }

    private Object getDefaultValue(Schema schema) {
        Schema.Type type = schema.getType();
        if (type == Schema.Type.UNION) {
            return null;
        }
        
        return switch (type) {
            case STRING -> "";
            case INT -> 0;
            case LONG -> 0L;
            case FLOAT -> 0.0f;
            case DOUBLE -> 0.0;
            case BOOLEAN -> false;
            default -> null;
        };
    }

    private ParquetWriter<GenericRecord> getWriter(String sourceFile) {
        return writers.computeIfAbsent(sourceFile, this::createWriter);
    }

    public void close() {
        try {
            // Flush remaining batches
            new ArrayList<>(batchMap.keySet()).forEach(writerKey -> {
                try {
                    flushBatch(writerKey);
                } catch (Exception e) {
                    logger.error("Final flush error for {}", writerKey, e);
                }
            });
            
            // Close all writers with proper error handling
            new ArrayList<>(writers.keySet()).forEach(this::closeWriter);
        } finally {
            writers.clear();
            batchMap.clear();
            logger.info("All resources cleaned up");
        }
    }

    public List<String> getAvroSchemaFields() {
        List<String> fields = new ArrayList<>();
        for (Schema.Field field : avroSchema.getFields()) {
            fields.add(field.name());
        }
        return fields;
    }
} 