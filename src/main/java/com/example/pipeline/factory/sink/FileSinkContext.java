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
 * This context maintains a map of BufferedWriter instances for each output file
 * and handles both TEXT and CSV file formats.
 * 
 * Behavior:
 * - Each processed file is written to its own output file
 * - Output files are named with pattern: <prefix>_<sourcefilename>_<timestamp>.<extension>
 * - CSV files can be transformed based on column mapping
 * - Writers are created per file and closed after writing
 */
public class FileSinkContext implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(FileSinkContext.class);
    
    // Base directory where all output files will be written
    private final String directory;
    
    // Prefix to be added to all output filenames
    private final String prefix;
    
    // File extension to be used for output files
    private final String extension;
    
    // Whether to include headers in CSV output
    private final boolean includeHeaders;
    
    // Helper class for CSV transformations
    private final CsvTransformer csvTransformer;
    
    // Output format (e.g., "csv", "text")
    private final String format;

    /**
     * Initializes the FileSinkContext with configuration for file output.
     * Creates the output directory if it doesn't exist.
     *
     * @param directory Output directory path
     * @param prefix Prefix for output filenames
     * @param extension File extension for output files
     * @param includeHeaders Whether to include headers in CSV output
     * @param columnMapping Column mapping configuration for CSV transformation
     * @param format Output format ("csv" or "text")
     */
    public FileSinkContext(String directory, String prefix, String extension, boolean includeHeaders, 
                          Map<String, String> columnMapping, String format) {
        this.directory = directory;
        this.prefix = prefix;
        this.extension = extension;
        this.includeHeaders = includeHeaders;
        this.format = format;
        this.csvTransformer = new CsvTransformer(columnMapping, includeHeaders);
        
        logger.debug("Initializing FileSinkContext with directory: {}, prefix: {}, extension: {}, includeHeaders: {}, format: {}", 
            directory, prefix, extension, includeHeaders, format);
        
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
     * Behavior:
     * - Creates a unique output file for each processed item
     * - Output filename format: <prefix>_<sourcefilename>_<timestamp>.<extension>
     * - CSV files are transformed if format is "csv"
     * - Text files are wrapped with "processed-" and "-done"
     * - Writers are created and closed for each file
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
            String fileType = parts[1].substring("TYPE=".length());
            String content = parts[2];
            
            logger.debug("Parsed metadata - sourceFile: {}, fileType: {}, content length: {}", 
                sourceFile, fileType, content.length());
            
            // Create output filename with timestamp
            // Format: <directory>/<prefix>_<sourcefilename>_<timestamp>.<extension>
            String timestamp = String.format("%tY%<tm%<td_%<tH%<tM%<tS", new java.util.Date());
            String outputFile = String.format("%s/%s_%s_%s%s",
                directory, prefix, 
                sourceFile.replace(fileType.equals("CSV") ? ".csv" : ".txt", ""),
                timestamp,
                extension);
            
            logger.debug("Creating output file: {}", outputFile);
            
            // Create a new writer for this file
            writer = new BufferedWriter(new FileWriter(outputFile));
            
            if (fileType.equals("TEXT")) {
                // Text file processing - wrap content with markers
                content = "processed-" + content + "-done";
                logger.debug("Processing TEXT file, modified content: {}", content);
                writer.write(content);
            } else if (fileType.equals("CSV")) {
                // CSV file processing
                if ("csv".equals(format)) {
                    // Transform CSV content based on column mapping
                    content = csvTransformer.transform(content);
                    logger.debug("Transformed CSV content");
                }
                writer.write(content);
            }
            
            writer.flush();
            logger.info("Successfully wrote processed content to: {}", outputFile);
            
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

    /**
     * No-op method maintained for compatibility.
     * Writers are now created and closed per file in the write() method.
     */
    public void close() {
        logger.debug("No writers to close - writers are created and closed per file");
    }
}