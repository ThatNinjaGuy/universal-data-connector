package com.example.pipeline.service;

import com.example.pipeline.config.PipelineConfig;
import com.example.pipeline.pipeline.PipelineBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
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
    private final JetService jetService;

    public PipelineService(HazelcastInstance hazelcastInstance) {
        this.jetService = hazelcastInstance.getJet();
    }

    public void startPipeline(String configFile) {
        try {
            logger.info("Loading pipeline configuration from: {}", configFile);
            PipelineConfig config = loadConfig(configFile);
            
            if (config.getPipelines() == null || config.getPipelines().isEmpty()) {
                throw new IllegalArgumentException("No pipelines defined in configuration");
            }

            for (PipelineConfig.Pipeline pipelineConfig : config.getPipelines()) {
                logger.info("Starting pipeline: {}", pipelineConfig.getName());
                PipelineBuilder builder = new PipelineBuilder(pipelineConfig);
                Pipeline pipeline = builder.build();      
                jetService.newJob(pipeline);
                logger.info("Pipeline {} started successfully", pipelineConfig.getName());
            }
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