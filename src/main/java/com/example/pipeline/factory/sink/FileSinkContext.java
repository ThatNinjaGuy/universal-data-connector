package com.example.pipeline.factory.sink;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles writing processed data to files in the specified output directory.
 * This context maintains a map of BufferedWriter instances for each output file.
 * 
 * Behavior:
 * - Each processed file is written to its own output file
 * - Output files preserve the original source filename
 * - Writers are created per file and closed after writing
 */
public class FileSinkContext implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(FileSinkContext.class);
    
    // Base directory where all output files will be written
    private final String directory;
    
    // File extension to be used for output files
    private final String extension;
    
    // Whether to include headers in CSV output
    private final boolean includeHeaders;

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
        
        logger.debug("Initializing FileSinkContext with directory: {}, extension: {}, includeHeaders: {}", 
            directory, extension, includeHeaders);
        
        // Create output directory if it doesn't exist
        File outDir = new File(directory);
        if (!outDir.exists()) {
            outDir.mkdirs();
            logger.info("Created output directory: {}", directory);
        }
    }

    /**
     * Processes and writes a single item to its own output file.
     * The item is expected to be in the format: "SOURCE=<filename>|TYPE=<filetype>|<content>"
     *
     * @param item The item to process and write, containing metadata and content
     */
    public void write(String item) {
        BufferedWriter writer = null;
        try {
            logger.debug("Processing item: {}", item);
            
            // Parse metadata from the item
            // Format: SOURCE=<filename>|TYPE=<filetype>|<content>
            String[] parts = item.split("\\|", -1);
            if (parts.length < 3) {
                logger.error("Invalid item format: {}", item);
                return;
            }
            
            String sourceFile = parts[0].substring("SOURCE=".length());
            String content = parts[2];
            
            // Remove the original extension before adding the new one
            String baseFilename = sourceFile;
            int lastDotIndex = sourceFile.lastIndexOf('.');
            if (lastDotIndex > 0) {
                baseFilename = sourceFile.substring(0, lastDotIndex);
            }
            
            String outputFile = String.format("%s/%s%s", directory, baseFilename, extension);
            logger.debug("Creating output file: {}", outputFile);
            
            // Create a new writer for this file
            writer = new BufferedWriter(new FileWriter(outputFile));
            writer.write(content);
            writer.flush();
            
            logger.info("Successfully wrote content to: {}", outputFile);
            
        } catch (Exception e) {
            logger.error("Error processing item: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process item", e);
        } finally {
            // Always close the writer
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    logger.error("Failed to close writer: {}", e.getMessage());
                }
            }
        }
    }
}