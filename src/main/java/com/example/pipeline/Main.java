package com.example.pipeline;

import com.example.pipeline.config.ConfigurationLoader;
import com.example.pipeline.config.PipelineConfig;
import com.example.pipeline.pipeline.PipelineBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    
    public static void main(String[] args) {
        if (args.length < 1) {
            logger.error("Configuration file path is required as argument");
            System.exit(1);
        }

        String configPath = args[0];
        HazelcastInstance hazelcastInstance = null;

        try {
            // Load pipeline configuration
            PipelineConfig pipelineConfig = ConfigurationLoader.load(configPath);
            logger.info("Loaded pipeline configuration from: {}", configPath);

            // Create and configure Hazelcast instance
            hazelcastInstance = createHazelcastInstance();
            logger.info("Hazelcast instance started successfully");

            // Build and submit the pipeline
            PipelineBuilder builder = new PipelineBuilder(pipelineConfig);
            Pipeline pipeline = builder.build();
            submitPipeline(hazelcastInstance, pipeline);

            // Add shutdown hook for graceful shutdown
            addShutdownHook(hazelcastInstance);

            // Keep the application running
            Thread.currentThread().join();

        } catch (IOException e) {
            logger.error("Failed to load configuration: {}", e.getMessage(), e);
            System.exit(1);
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
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setEnabled(true);
        jetConfig.setResourceUploadEnabled(true);
        
        // Configure instance
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

    private static void submitPipeline(HazelcastInstance hazelcastInstance, Pipeline pipeline) {
        JetService jet = hazelcastInstance.getJet();
        
        CompletableFuture.runAsync(() -> {
            try {
                jet.newJob(pipeline).join();
                logger.info("Pipeline job submitted successfully");
            } catch (Exception e) {
                logger.error("Failed to submit pipeline job: {}", e.getMessage(), e);
                hazelcastInstance.shutdown();
                System.exit(1);
            }
        });
    }

    private static void addShutdownHook(HazelcastInstance hazelcastInstance) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Hazelcast instance...");
            try {
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