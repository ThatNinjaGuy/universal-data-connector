package com.example.pipeline.pipeline;

import com.example.pipeline.config.ConfigurationLoader;
import com.example.pipeline.config.PipelineConfig;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PipelineManager {
    private static final Logger logger = LoggerFactory.getLogger(PipelineManager.class);
    private final JetService jetService;
    private final Map<String, Job> runningJobs;

    public PipelineManager(JetService jetService) {
        this.jetService = jetService;
        this.runningJobs = new ConcurrentHashMap<>();
    }

    public void startAllPipelines(String configPath) {
        try {
            logger.info("Starting to look for pipeline configurations in: {}", configPath);
            PipelineConfig config = ConfigurationLoader.load(configPath);
            startPipelinesFromConfig(config, configPath);
        } catch (Exception e) {
            logger.error("Failed to start pipelines: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to start pipelines", e);
        }
    }

    private void startPipelinesFromConfig(PipelineConfig config, String configPath) {
        if (config.getPipelines() == null || config.getPipelines().isEmpty()) {
            logger.error("No pipelines found in configuration: {}", configPath);
            return;
        }

        for (PipelineConfig.Pipeline pipelineConfig : config.getPipelines()) {
            try {
                startSinglePipeline(pipelineConfig);
            } catch (Exception e) {
                logger.error("Failed to start pipeline '{}': {}", pipelineConfig.getName(), e.getMessage(), e);
            }
        }
    }

    private void startSinglePipeline(PipelineConfig.Pipeline pipelineConfig) {
        PipelineBuilder builder = new PipelineBuilder(pipelineConfig);
        Pipeline pipeline = builder.build();
        
        String jobName = pipelineConfig.getName();
        if (jobName == null || jobName.isEmpty()) {
            jobName = "pipeline-" + runningJobs.size();
        }
        jobName = jobName + "-" + System.currentTimeMillis();
        
        JobConfig jobConfig = new JobConfig()
                .setName(jobName)
                .setAutoScaling(true)
                .setMetricsEnabled(true)
                .setStoreMetricsAfterJobCompletion(true);
        Job job = jetService.newJob(pipeline, jobConfig);
        runningJobs.put(jobName, job);
        
        logger.info("Started pipeline job '{}'", jobName);
    }

    public void stopPipeline(String jobName) {
        Job job = runningJobs.get(jobName);
        if (job != null) {
            job.cancel();
            runningJobs.remove(jobName);
            logger.info("Stopped pipeline job: {}", jobName);
        }
    }

    public void stopAllPipelines() {
        runningJobs.forEach((name, job) -> {
            job.cancel();
            logger.info("Stopped pipeline job: {}", name);
        });
        runningJobs.clear();
    }

    public boolean isPipelineRunning(String jobName) {
        Job job = runningJobs.get(jobName);
        return job != null && job.getStatus() == JobStatus.RUNNING;
    }

    public Map<String, Job> getRunningJobs() {
        return new ConcurrentHashMap<>(runningJobs);
    }
} 