package com.example.pipeline.dto;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;

public class JobInfo {
    private final String name;
    private final JobStatus status;
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