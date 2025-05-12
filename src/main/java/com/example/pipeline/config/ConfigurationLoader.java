package com.example.pipeline.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class ConfigurationLoader {
    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    
    public static PipelineConfig load(String configPath) throws IOException {
        try {
            // First try to load from classpath
            InputStream inputStream = ConfigurationLoader.class
                .getClassLoader()
                .getResourceAsStream(configPath);
                
            if (inputStream != null) {
                return mapper.readValue(inputStream, PipelineConfig.class);
            }
            
            // If not found in classpath, try as file path
            return mapper.readValue(new File(configPath), PipelineConfig.class);
        } catch (IOException e) {
            throw new IOException("Failed to load pipeline configuration: " + e.getMessage(), e);
        }
    }
    
    public static PipelineConfig load(InputStream inputStream) throws IOException {
        try {
            return mapper.readValue(inputStream, PipelineConfig.class);
        } catch (IOException e) {
            throw new IOException("Failed to load pipeline configuration from input stream: " + e.getMessage(), e);
        }
    }
} 