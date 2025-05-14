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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ParquetSinkContext implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ParquetSinkContext.class);
    private final String directory;
    private final String schema;
    private final Map<String, ParquetWriter<GenericRecord>> writers = new HashMap<>();
    private transient Schema avroSchema;
    
    public ParquetSinkContext(String directory, String schema) {
        this.directory = directory;
        this.schema = schema;
    }

    public void write(String csvContent, Map<String, Integer> headerMap, String sourceFile) {
        ParquetWriter<GenericRecord> writer = null;
        try {
            writer = writers.computeIfAbsent(sourceFile, this::createWriter);
            
            String[] lines = csvContent.split("\n", -1);
            logger.info("Processing {} lines from {}", lines.length, sourceFile);
            
            // Skip header line (i=0) and process data lines
            for (int i = 1; i < lines.length; i++) {
                String line = lines[i].trim();
                if (line.isEmpty()) continue;
                
                String[] values = line.split(",", -1);
                GenericRecord record = new GenericData.Record(avroSchema);
                
                logger.info("Processing line {}/{}: {}", i, lines.length, line);
                
                // Map fields using header positions
                for (Schema.Field field : avroSchema.getFields()) {
                    String fieldName = field.name();
                    Integer columnIndex = headerMap.get(fieldName);
                    
                    if (columnIndex != null && columnIndex < values.length) {
                        String value = values[columnIndex].trim();
                        record.put(fieldName, value);
                        logger.debug("Set field {} = '{}'", fieldName, value);
                    }
                }
                
                writer.write(record);
                logger.info("Wrote record from line {}: {}", i, record);
            }
            
            // Close and remove the writer after processing all lines
            writer.close();
            writers.remove(sourceFile);
            logger.info("Closed writer for {}", sourceFile);
            
        } catch (Exception e) {
            logger.error("Failed to write to Parquet file: {}", e.getMessage(), e);
            if (writer != null) {
                try {
                    writer.close();
                } catch (Exception ce) {
                    logger.error("Failed to close writer on error: {}", ce.getMessage());
                }
            }
            throw new RuntimeException("Failed to write to Parquet file", e);
        }
    }

    private ParquetWriter<GenericRecord> createWriter(String sourceFile) {
        try {
            if (avroSchema == null) {
                avroSchema = new Schema.Parser().parse(schema);
            }

            // Create absolute paths
            File baseDir = new File(directory).getAbsoluteFile();
            if (!baseDir.exists()) {
                baseDir.mkdirs();
                logger.info("Created output directory: {}", baseDir);
            }

            String timestamp = String.format("%tY%<tm%<td_%<tH%<tM%<tS", new Date());
            String outputFile = String.format("%s/processed_%s_%s.parquet",
                baseDir.getPath(),
                sourceFile.replace(".csv", ""),
                timestamp);

            Path path = new Path("file://" + outputFile);
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "file:///");
            
            logger.info("Creating Parquet writer at absolute path: {}", outputFile);

            ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
                .withSchema(avroSchema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withConf(conf)
                .build();

            logger.info("Created Parquet writer for source {} at path: {}", sourceFile, outputFile);
            return writer;
        } catch (Exception e) {
            logger.error("Failed to create writer for {}: {}", sourceFile, e.getMessage(), e);
            throw new RuntimeException("Failed to create writer for " + sourceFile, e);
        }
    }

    public void close() {
        writers.forEach((file, writer) -> {
            try {
                writer.close();
                logger.info("Closed writer for {}", file);
            } catch (Exception e) {
                logger.error("Failed to close writer for {}: {}", file, e.getMessage());
            }
        });
        writers.clear();
    }
} 