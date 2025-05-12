package com.example.pipeline.service;

import com.example.pipeline.config.PipelineConfig;
import com.example.pipeline.pipeline.PipelineBuilder;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;

@Service
public class PipelineService {
    private static final Logger logger = LoggerFactory.getLogger(PipelineService.class);
    private final JetInstance jetInstance;

    public PipelineService(JetInstance jetInstance) {
        this.jetInstance = jetInstance;
    }

    public void startPipeline(String configFile) {
        try {
            logger.info("Loading pipeline configuration from: {}", configFile);
            PipelineConfig config = loadConfig(configFile);
            
            PipelineBuilder builder = new PipelineBuilder(config);
            Pipeline pipeline = builder.build();
            
            jetInstance.newJob(pipeline);
            logger.info("Pipeline started successfully");
        } catch (Exception e) {
            throw new RuntimeException("Pipeline execution failed: " + e.getMessage(), e);
        }
    }

    private PipelineConfig loadConfig(String configFile) {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(configFile)) {
            if (input == null) {
                throw new IllegalArgumentException("Config file not found: " + configFile);
            }
            return new Yaml(new Constructor(PipelineConfig.class)).load(input);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config: " + e.getMessage(), e);
        }
    }
} 