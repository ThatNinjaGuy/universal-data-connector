package com.example.pipeline.factory;

import com.hazelcast.function.SupplierEx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class FileSourceContext implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(FileSourceContext.class);
    private final BufferedReader reader;

    public FileSourceContext(String path) throws IOException {
        this.reader = new BufferedReader(new FileReader(path));
    }

    public String readLine() throws IOException {
        return reader.readLine();
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
} 