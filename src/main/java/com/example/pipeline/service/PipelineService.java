package com.example.pipeline.service;

import com.example.pipeline.dto.JobInfo;
import com.example.pipeline.pipeline.PipelineManager;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.stream.Collectors;

@Service
public class PipelineService {
    private static final Logger logger = LoggerFactory.getLogger(PipelineService.class);
    private final PipelineManager pipelineManager;

    public PipelineService(HazelcastInstance hazelcastInstance) {
        JetService jetService = hazelcastInstance.getJet();
        this.pipelineManager = new PipelineManager(jetService);
    }

    public void startAllPipelines(String configPath) {
        try {
            logger.info("Starting pipeline with configuration from: {}", configPath);
            pipelineManager.startAllPipelines(configPath);
        } catch (Exception e) {
            throw new RuntimeException("Pipeline execution failed: " + e.getMessage(), e);
        }
    }

    public void stopPipeline(String jobName) {
        try {
            logger.info("Stopping pipeline: {}", jobName);
            pipelineManager.stopPipeline(jobName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to stop pipeline: " + e.getMessage(), e);
        }
    }

    public void stopAllPipelines() {
        try {
            logger.info("Stopping all running pipelines");
            pipelineManager.stopAllPipelines();
        } catch (Exception e) {
            throw new RuntimeException("Failed to stop all pipelines: " + e.getMessage(), e);
        }
    }

    public boolean isPipelineRunning(String jobName) {
        return pipelineManager.isPipelineRunning(jobName);
    }

    public Map<String, JobInfo> getRunningJobs() {
        return pipelineManager.getRunningJobs().entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> new JobInfo(entry.getValue())
            ));
    }

    public int getRunningPipelineCount() {
        return pipelineManager.getRunningJobs().size();
    }

    public boolean hasRunningPipelines() {
        return !pipelineManager.getRunningJobs().isEmpty();
    }
} 