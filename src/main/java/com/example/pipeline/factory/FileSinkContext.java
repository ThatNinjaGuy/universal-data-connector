package com.example.pipeline.factory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class FileSinkContext implements AutoCloseable {
    private final BufferedWriter writer;

    public FileSinkContext(String path) throws IOException {
        try {
            this.writer = new BufferedWriter(new FileWriter(path, true));
        } catch (IOException e) {
            throw new IOException("Failed to initialize file sink: " + e.getMessage(), e);
        }
    }

    public void receive(String item) throws IOException {
        try {
            writer.write(item);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new IOException("Failed to write to file: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }
} 