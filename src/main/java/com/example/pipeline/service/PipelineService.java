package com.example.pipeline.service;

import com.example.pipeline.pipeline.PipelineManager;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class PipelineService {
    private static final Logger logger = LoggerFactory.getLogger(PipelineService.class);
    private final PipelineManager pipelineManager;

    public PipelineService(HazelcastInstance hazelcastInstance) {
        JetService jetService = hazelcastInstance.getJet();
        this.pipelineManager = new PipelineManager(jetService);
    }

    public void startAllPipelines(String configDirectory) {
        try {
            logger.info("Starting all pipelines from configuration directory: {}", configDirectory);
            pipelineManager.startAllPipelines(configDirectory);
        } catch (Exception e) {
            throw new RuntimeException("Failed to start all pipelines: " + e.getMessage(), e);
        }
    }

    public void startPipelines(String configFile) {
        try {
            logger.info("Starting pipeline with configuration from: {}", configFile);
            pipelineManager.startPipelines(configFile);
        } catch (Exception e) {
            throw new RuntimeException("Pipeline execution failed: " + e.getMessage(), e);
        }
    }

    public void stopPipeline(String jobName) {
        pipelineManager.stopPipeline(jobName);
    }

    public void stopAllPipelines() {
        pipelineManager.stopAllPipelines();
    }

    public boolean isPipelineRunning(String jobName) {
        return pipelineManager.isPipelineRunning(jobName);
    }
} 