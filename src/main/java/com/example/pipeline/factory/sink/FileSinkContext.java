package com.example.pipeline.factory.sink;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.io.File;
import java.util.Map;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles writing processed data to files in the specified output directory.
 * This context maintains a map of BufferedWriter instances for each output file.
 * 
 * Behavior:
 * - Each processed file is written to its own output file
 * - Output files preserve the original source filename
 * - Writers are managed per file and closed after writing
 */
public class FileSinkContext implements SinkContext<String>, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(FileSinkContext.class);
    
    // Base directory where all output files will be written
    private final String directory;
    
    // File extension to be used for output files
    private final String extension;
    
    // Whether to include headers in CSV output
    private final boolean includeHeaders;
    
    // Map of writers for each source file
    private final Map<String, BufferedWriter> writers = new HashMap<>();

    /**
     * Initializes the FileSinkContext with configuration for file output.
     * Creates the output directory if it doesn't exist.
     *
     * @param directory Output directory path
     * @param extension File extension for output files
     * @param includeHeaders Whether to include headers in CSV output
     */
    public FileSinkContext(String directory, String extension, boolean includeHeaders) {
        this.directory = directory;
        this.extension = extension;
        this.includeHeaders = includeHeaders;
    }

    @Override
    public void init() {
        logger.debug("Initializing FileSinkContext with directory: {}, extension: {}, includeHeaders: {}", 
            directory, extension, includeHeaders);
        
        // Create output directory if it doesn't exist
        File outDir = new File(directory);
        if (!outDir.exists()) {
            outDir.mkdirs();
            logger.info("Created output directory: {}", directory);
        }
    }

    @Override
    public void receive(String item) {
        try {
            // Parse metadata from the item
            // Format: SOURCE=<filename>|TYPE=<filetype>|<content>
            String[] parts = item.split("\\|", -1);
            if (parts.length < 3) {
                logger.error("Invalid item format: {}", item);
                return;
            }
            
            String sourceFile = parts[0].substring("SOURCE=".length());
            String content = parts[2];
            
            // Get or create writer for this source file
            BufferedWriter writer = writers.computeIfAbsent(sourceFile, filename -> {
                try {
                    String outputFile;
                    if (extension.isEmpty()) {
                        // If extension is empty, preserve the original extension
                        outputFile = String.format("%s/%s", directory, filename);
                    } else {
                        // Remove the original extension before adding the new one
                        String baseFilename = filename;
                        int lastDotIndex = filename.lastIndexOf('.');
                        if (lastDotIndex > 0) {
                            baseFilename = filename.substring(0, lastDotIndex);
                        }
                        outputFile = String.format("%s/%s%s", directory, baseFilename, extension);
                    }
                    logger.info("Creating new file: {}", outputFile);
                    return new BufferedWriter(new FileWriter(outputFile));
                } catch (IOException e) {
                    logger.error("Failed to create writer for file {}: {}", filename, e.getMessage());
                    throw new RuntimeException("Failed to create writer", e);
                }
            });

            writer.write(content);
            writer.newLine();
            writer.flush();
            
            logger.info("Successfully wrote content to file: {}", sourceFile);
            
        } catch (Exception e) {
            logger.error("Error processing item: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process item", e);
        }
    }

    @Override
    public void close() {
        try {
            for (BufferedWriter writer : writers.values()) {
                if (writer != null) {
                    writer.close();
                }
            }
            writers.clear();
            logger.info("Successfully closed all writers");
        } catch (IOException e) {
            logger.error("Failed to close writers: {}", e.getMessage());
            throw new RuntimeException("Failed to close writers", e);
        }
    }
}