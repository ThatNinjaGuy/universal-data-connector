package com.example.pipeline.dto;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Information about a pipeline job")
public class JobInfo {
    @Schema(description = "Name of the pipeline job", example = "pipeline-1")
    private final String name;

    @Schema(description = "Current status of the job", example = "RUNNING")
    private final JobStatus status;

    @Schema(description = "Timestamp when the job was submitted", example = "1234567890")
    private final long submissionTime;

    public JobInfo(Job job) {
        this.name = job.getName();
        this.status = job.getStatus();
        this.submissionTime = job.getSubmissionTime();
    }

    public String getName() {
        return name;
    }

    public JobStatus getStatus() {
        return status;
    }

    public long getSubmissionTime() {
        return submissionTime;
    }
} 