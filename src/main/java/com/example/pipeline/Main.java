package com.example.pipeline;

import com.example.pipeline.pipeline.PipelineManager;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    
    public static void main(String[] args) {
        if (args.length < 1) {
            logger.error("Configuration directory path is required as argument");
            System.exit(1);
        }

        String configDir = args[0];
        HazelcastInstance hazelcastInstance = null;

        try {
            // Create and configure Hazelcast instance
            hazelcastInstance = createHazelcastInstance();
            logger.info("Hazelcast instance started successfully");

            // Get Jet service
            JetService jetService = hazelcastInstance.getJet();

            // Create pipeline manager and start all pipelines
            PipelineManager pipelineManager = new PipelineManager(jetService);
            pipelineManager.startAllPipelines(configDir);

            // Add shutdown hook for graceful shutdown
            addShutdownHook(hazelcastInstance, pipelineManager);

            // Keep the application running
            Thread.currentThread().join();

        } catch (Exception e) {
            logger.error("Pipeline execution failed: {}", e.getMessage(), e);
            System.exit(1);
        } finally {
            // Cleanup if not handled by shutdown hook
            if (hazelcastInstance != null && hazelcastInstance.getLifecycleService().isRunning()) {
                hazelcastInstance.shutdown();
            }
        }
    }

    private static HazelcastInstance createHazelcastInstance() {
        Config config = new Config();
        config.setClusterName("data-pipeline-cluster");
        
        // Configure Jet
        config.getJetConfig()
            .setEnabled(true)
            .setResourceUploadEnabled(true);
        
        // Configure networking
        config.getNetworkConfig()
            .setPort(5701)
            .getJoin()
            .getMulticastConfig()
            .setEnabled(false);
        config.getNetworkConfig()
            .getJoin()
            .getTcpIpConfig()
            .setEnabled(true)
            .addMember("localhost");

        return Hazelcast.newHazelcastInstance(config);
    }

    private static void addShutdownHook(HazelcastInstance hazelcastInstance, PipelineManager pipelineManager) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down all pipelines...");
            try {
                pipelineManager.stopAllPipelines();
                
                if (hazelcastInstance != null && hazelcastInstance.getLifecycleService().isRunning()) {
                    hazelcastInstance.shutdown();
                    logger.info("Hazelcast instance shut down successfully");
                }
            } catch (Exception e) {
                logger.error("Error during shutdown: {}", e.getMessage(), e);
            }
        }));
    }
} 