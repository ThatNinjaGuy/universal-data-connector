package com.example.pipeline.factory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.io.File;
import java.util.Map;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSinkContext implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(FileSinkContext.class);
    private final String directory;
    private final String prefix;
    private final String extension;
    private final boolean includeHeaders;
    private final Map<String, BufferedWriter> writers;

    public FileSinkContext(String directory, String prefix, String extension, boolean includeHeaders) {
        this.directory = directory;
        this.prefix = prefix;
        this.extension = extension;
        this.includeHeaders = includeHeaders;
        this.writers = new HashMap<>();
        
        logger.debug("Initializing FileSinkContext with directory: {}, prefix: {}, extension: {}, includeHeaders: {}", 
            directory, prefix, extension, includeHeaders);
        
        // Create output directory if it doesn't exist
        File outDir = new File(directory);
        if (!outDir.exists()) {
            outDir.mkdirs();
            logger.info("Created output directory: {}", directory);
        }
    }

    public void write(String item) {
        try {
            logger.debug("Processing item: {}", item);
            
            // Parse metadata
            String[] parts = item.split("\\|", -1);
            if (parts.length < 3) {
                logger.error("Invalid item format: {}", item);
                return;
            }
            
            String sourceFile = parts[0].substring("SOURCE=".length());
            String fileType = parts[1].substring("TYPE=".length());
            String content = parts[2];
            
            logger.debug("Parsed metadata - sourceFile: {}, fileType: {}, content length: {}", 
                sourceFile, fileType, content.length());
            
            // Create output filename with timestamp
            String timestamp = String.format("%tY%<tm%<td_%<tH%<tM%<tS", new java.util.Date());
            String outputFile = String.format("%s/%s_%s_%s%s",
                directory, prefix, 
                sourceFile.replace(fileType.equals("CSV") ? ".csv" : ".txt", ""),
                timestamp,
                extension);
            
            logger.debug("Created output filename: {}", outputFile);
            
            if (fileType.equals("TEXT")) {
                // Text file processing
                content = "processed-" + content + "-done";
                logger.debug("Processing TEXT file, modified content: {}", content);
                writeToFile(outputFile, content + "\n");
            } else if (fileType.equals("CSV")) {
                // CSV file processing
                String[] lines = content.split("\n", -1);
                logger.debug("Processing CSV file with {} lines", lines.length);
                
                if (lines.length > 0) {
                    StringBuilder processedContent = new StringBuilder();
                    
                    // Handle headers
                    if (includeHeaders && lines.length > 0) {
                        String header = lines[0];
                        processedContent.append(header).append("\n");
                        logger.debug("Added CSV header: {}", header);
                    }
                    
                    // Process data rows
                    for (int i = 1; i < lines.length; i++) {
                        String line = lines[i].trim();
                        if (!line.isEmpty()) {
                            processedContent.append(line).append("\n");
                            logger.debug("Added CSV line {}: {}", i, line);
                        }
                    }
                    
                    writeToFile(outputFile, processedContent.toString());
                    logger.info("Wrote processed CSV content to: {}", outputFile);
                }
            }
        } catch (Exception e) {
            logger.error("Error processing item: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process item", e);
        }
    }

    private void writeToFile(String filename, String content) throws IOException {
        logger.debug("Writing {} bytes to file: {}", content.length(), filename);
        BufferedWriter writer = writers.computeIfAbsent(filename, this::createWriter);
        writer.write(content);
        writer.flush();
        logger.debug("Successfully wrote to file: {}", filename);
    }

    private BufferedWriter createWriter(String filename) {
        try {
            logger.debug("Creating new writer for file: {}", filename);
            return new BufferedWriter(new FileWriter(filename, true));
        } catch (IOException e) {
            logger.error("Failed to create writer for file: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void close() {
        logger.debug("Closing {} writers", writers.size());
        writers.values().forEach(writer -> {
            try {
                writer.close();
            } catch (IOException e) {
                logger.error("Failed to close writer: {}", e.getMessage());
            }
        });
        writers.clear();
        logger.debug("All writers closed and cleared");
    }
}