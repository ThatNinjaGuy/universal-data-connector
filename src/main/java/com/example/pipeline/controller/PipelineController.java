package com.example.pipeline.controller;

import com.example.pipeline.dto.JobInfo;
import com.example.pipeline.service.PipelineService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/pipelines")
public class PipelineController {
    private static final Logger logger = LoggerFactory.getLogger(PipelineController.class);
    private final PipelineService pipelineService;

    public PipelineController(PipelineService pipelineService) {
        this.pipelineService = pipelineService;
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getPipelineStatus() {
        logger.info("Getting pipeline status");
        Map<String, Object> status = Map.of(
            "runningCount", pipelineService.getRunningPipelineCount(),
            "hasRunningPipelines", pipelineService.hasRunningPipelines()
        );
        return ResponseEntity.ok(status);
    }

    @GetMapping("/jobs")
    public ResponseEntity<Map<String, JobInfo>> getRunningJobs() {
        logger.info("Getting all running pipeline jobs");
        Map<String, JobInfo> jobs = pipelineService.getRunningJobs();
        
        return ResponseEntity.ok(jobs);
    }

    @GetMapping("/jobs/{jobName}/status")
    public ResponseEntity<Boolean> isPipelineRunning(@PathVariable String jobName) {
        logger.info("Checking status for pipeline: {}", jobName);
        return ResponseEntity.ok(pipelineService.isPipelineRunning(jobName));
    }

    @GetMapping("/count")
    public ResponseEntity<Integer> getRunningPipelineCount() {
        logger.info("Getting count of running pipelines");
        return ResponseEntity.ok(pipelineService.getRunningPipelineCount());
    }
} 