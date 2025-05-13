package com.example.pipeline.factory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSourceContext implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(FileSourceContext.class);
    private final String directoryPath;
    private transient WatchService watchService;
    private transient Path directory;

    public FileSourceContext(String directoryPath) {
        this.directoryPath = directoryPath;
    }

    public void init() throws IOException {
        directory = Paths.get(directoryPath);
        watchService = FileSystems.getDefault().newWatchService();
        directory.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, 
                         StandardWatchEventKinds.ENTRY_MODIFY);
    }

    public void processFile(Path file) {
        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Process line
                logger.debug("Read line: {}", line);
            }
        } catch (IOException e) {
            logger.error("Failed to read file: {}", e.getMessage());
        }
    }

    public void close() {
        if (watchService != null) {
            try {
                watchService.close();
            } catch (IOException e) {
                logger.error("Failed to close watch service: {}", e.getMessage());
            }
        }
    }
} 