package com.example.pipeline.controller;

import com.example.pipeline.dto.JobInfo;
import com.example.pipeline.service.PipelineService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/pipelines")
@Tag(name = "Pipeline Management", description = "APIs for managing and monitoring data pipelines")
public class PipelineController {
    private static final Logger logger = LoggerFactory.getLogger(PipelineController.class);
    private final PipelineService pipelineService;

    public PipelineController(PipelineService pipelineService) {
        this.pipelineService = pipelineService;
    }

    @GetMapping("/status")
    @Operation(
        summary = "Get overall pipeline status",
        description = "Returns the current status of all pipelines including count and running state",
        responses = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved pipeline status"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
        }
    )
    public ResponseEntity<Map<String, Object>> getPipelineStatus() {
        logger.info("Getting pipeline status");
        Map<String, Object> status = Map.of(
            "runningCount", pipelineService.getRunningPipelineCount(),
            "hasRunningPipelines", pipelineService.hasRunningPipelines()
        );
        return ResponseEntity.ok(status);
    }

    @GetMapping("/jobs")
    @Operation(
        summary = "Get all running jobs",
        description = "Returns detailed information about all currently running pipeline jobs",
        responses = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved running jobs"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
        }
    )
    public ResponseEntity<Map<String, JobInfo>> getRunningJobs() {
        logger.info("Getting all running pipeline jobs");
        Map<String, JobInfo> jobs = pipelineService.getRunningJobs();
        return ResponseEntity.ok(jobs);
    }

    @GetMapping("/jobs/{jobName}/status")
    @Operation(
        summary = "Check pipeline status",
        description = "Checks if a specific pipeline is currently running",
        responses = {
            @ApiResponse(responseCode = "200", description = "Successfully checked pipeline status"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
        }
    )
    public ResponseEntity<Boolean> isPipelineRunning(
        @Parameter(description = "Name of the pipeline to check", required = true)
        @PathVariable String jobName
    ) {
        logger.info("Checking status for pipeline: {}", jobName);
        return ResponseEntity.ok(pipelineService.isPipelineRunning(jobName));
    }

    @GetMapping("/count")
    @Operation(
        summary = "Get running pipeline count",
        description = "Returns the total number of currently running pipelines",
        responses = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved pipeline count"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
        }
    )
    public ResponseEntity<Integer> getRunningPipelineCount() {
        logger.info("Getting count of running pipelines");
        return ResponseEntity.ok(pipelineService.getRunningPipelineCount());
    }

    @PostMapping("/start")
    @Operation(
        summary = "Start all pipelines",
        description = "Starts all pipelines using the provided configuration file",
        responses = {
            @ApiResponse(responseCode = "200", description = "Successfully started pipelines"),
            @ApiResponse(responseCode = "400", description = "Invalid configuration file path"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
        }
    )
    public ResponseEntity<Void> startAllPipelines(
        @Parameter(description = "Path to the configuration file", required = true)
        @RequestParam(required = true) String configFilePath
    ) {
        if (configFilePath == null || configFilePath.trim().isEmpty()) {
            throw new IllegalArgumentException("Configuration file path cannot be empty");
        }
        
        try {
            logger.info("Starting all pipelines with config file: {}", configFilePath);
            pipelineService.startAllPipelines(configFilePath.trim());
            return ResponseEntity.ok().build();
        } catch (IllegalArgumentException e) {
            logger.error("Invalid configuration file path: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            logger.error("Failed to start pipelines: {}", e.getMessage());
            throw new RuntimeException("Failed to start pipelines: " + e.getMessage(), e);
        }
    }

    @PostMapping("/stop/{jobName}")
    @Operation(
        summary = "Stop a specific pipeline",
        description = "Stops a specific pipeline by its job name",
        responses = {
            @ApiResponse(responseCode = "200", description = "Successfully stopped pipeline"),
            @ApiResponse(responseCode = "404", description = "Pipeline not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
        }
    )
    public ResponseEntity<Void> stopPipeline(
        @Parameter(description = "Name of the pipeline to stop", required = true)
        @PathVariable String jobName
    ) {
        logger.info("Stopping pipeline: {}", jobName);
        pipelineService.stopPipeline(jobName);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/stop-all")
    @Operation(
        summary = "Stop all pipelines",
        description = "Stops all currently running pipelines",
        responses = {
            @ApiResponse(responseCode = "200", description = "Successfully stopped all pipelines"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
        }
    )
    public ResponseEntity<Void> stopAllPipelines() {
        logger.info("Stopping all pipelines");
        pipelineService.stopAllPipelines();
        return ResponseEntity.ok().build();
    }
} 