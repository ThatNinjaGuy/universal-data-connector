package com.example.pipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import com.example.pipeline.service.PipelineService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication
@ComponentScan(basePackages = {
    "com.example.pipeline.config",
    "com.example.pipeline.service"
})
public class PipelineApplication {
    private static final Logger logger = LoggerFactory.getLogger(PipelineApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(PipelineApplication.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(PipelineService pipelineService) {
        return args -> {
            try {
                logger.info("Starting pipeline with configuration from: pipeline-config.yaml");
                pipelineService.startPipeline("pipeline-config.yaml");
            } catch (Exception e) {
                logger.error("Failed to start pipeline: {}", e.getMessage(), e);
                System.exit(1);
            }
        };
    }
} 