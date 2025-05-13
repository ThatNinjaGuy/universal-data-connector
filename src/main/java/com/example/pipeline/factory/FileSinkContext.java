package com.example.pipeline.factory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSinkContext implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(FileSinkContext.class);
    private final String filePath;
    private transient BufferedWriter writer;

    public FileSinkContext(String filePath) {
        this.filePath = filePath;
    }

    public void init() {
        try {
            writer = new BufferedWriter(new FileWriter(filePath, true)); // true for append mode
        } catch (IOException e) {
            logger.error("Failed to initialize file writer: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void write(String item) {
        try {
            writer.write(item);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            logger.error("Failed to write to file: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void close() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                logger.error("Failed to close file writer: {}", e.getMessage());
            }
        }
    }
}