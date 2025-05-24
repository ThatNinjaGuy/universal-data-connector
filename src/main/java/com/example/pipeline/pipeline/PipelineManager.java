package com.example.pipeline.pipeline;

import com.example.pipeline.config.ConfigurationLoader;
import com.example.pipeline.config.PipelineConfig;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

    public void startPipeline(String configPath) {
        try {
            PipelineConfig config = ConfigurationLoader.load(configPath);
            if (config.getPipelines() == null || config.getPipelines().isEmpty()) {
                logger.error("No pipelines found in configuration: {}", configPath);
                return;
            }

            for (PipelineConfig.Pipeline pipelineConfig : config.getPipelines()) {
                try {
                    PipelineBuilder builder = new PipelineBuilder(pipelineConfig);
                    Pipeline pipeline = builder.build();
                    
                    String jobName = pipelineConfig.getName();
                    if (jobName == null || jobName.isEmpty()) {
                        jobName = new File(configPath).getName() + "-" + runningJobs.size();
                    }
                    
                    Job job = jetService.newJob(pipeline);
                    runningJobs.put(jobName, job);
                    
                    logger.info("Started pipeline job '{}' from configuration: {}", jobName, configPath);
                } catch (Exception e) {
                    logger.error("Failed to start pipeline '{}': {}", pipelineConfig.getName(), e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to load configuration from {}: {}", configPath, e.getMessage(), e);
            throw new RuntimeException("Failed to start pipeline", e);
        }
    }

    public void startAllPipelines(String configDirectory) {
        try {
            List<String> configFiles = findConfigurationFiles(configDirectory);
            logger.info("Found {} pipeline configurations", configFiles.size());
            
            for (String configFile : configFiles) {
                startPipeline(configFile);
            }
        } catch (Exception e) {
            logger.error("Failed to start all pipelines: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to start all pipelines", e);
        }
    }

    private List<String> findConfigurationFiles(String directory) throws IOException {
        List<String> configFiles = new ArrayList<>();
        File dir = new File(directory);
        
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IOException("Invalid configuration directory: " + directory);
        }

        File[] files = dir.listFiles((d, name) -> name.endsWith(".yaml") || name.endsWith(".yml"));
        if (files != null) {
            for (File file : files) {
                configFiles.add(file.getAbsolutePath());
            }
        }
        
        return configFiles;
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