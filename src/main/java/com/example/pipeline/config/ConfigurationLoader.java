package com.example.pipeline.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ConfigurationLoader {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationLoader.class);
    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    
    public static PipelineConfig load(String configPath) throws IOException {
        try {
            InputStream inputStream = ConfigurationLoader.class
                .getClassLoader()
                .getResourceAsStream(configPath);
                
            if (inputStream == null) {
                throw new IOException("Configuration file not found: " + configPath);
            }
            
            return mapper.readValue(inputStream, PipelineConfig.class);
        } catch (IOException e) {
            throw new IOException("Failed to load pipeline configuration: " + e.getMessage(), e);
        }
    }
    
    public static List<String> findConfigurationFiles(String resourcePath) throws IOException {
        List<String> configFiles = new ArrayList<>();
        String yamlPath = resourcePath + "/pipeline-config.yaml";
        
        if (ConfigurationLoader.class.getClassLoader().getResource(yamlPath) != null) {
            logger.debug("Found configuration file: {}", yamlPath);
            configFiles.add(yamlPath);
        } else {
            throw new IOException("Configuration file not found: " + yamlPath);
        }
        
        return configFiles;
    }
} 