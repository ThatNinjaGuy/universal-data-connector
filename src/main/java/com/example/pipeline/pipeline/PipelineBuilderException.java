package com.example.pipeline.pipeline;

public class PipelineBuilderException extends RuntimeException {
    
    public PipelineBuilderException(String message) {
        super(message);
    }
    
    public PipelineBuilderException(String message, Throwable cause) {
        super(message, cause);
    }
} 