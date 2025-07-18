package com.example.pipeline.factory.sink;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.Map;

public class S3SinkContext implements SinkContext<String>, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(S3SinkContext.class);
    private final Map<String, String> properties;
    private AmazonS3 s3Client;
    private String bucketName;
    private String prefix;

    public S3SinkContext(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public void init() {
        try {
            // Initialize AWS credentials
            BasicAWSCredentials awsCredentials = new BasicAWSCredentials(
                properties.get("accessKey"),
                properties.get("secretKey")
            );

            // Create S3 client
            this.s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(properties.get("region"))
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();

            this.bucketName = properties.get("bucketName");
            this.prefix = properties.getOrDefault("prefix", "");

            // Verify bucket exists
            if (!s3Client.doesBucketExistV2(bucketName)) {
                throw new IllegalStateException("Bucket " + bucketName + " does not exist");
            }

            logger.info("Initialized S3SinkContext for bucket: {}", bucketName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize S3 sink: " + e.getMessage(), e);
        }
    }

    @Override
    public void receive(String item) {
        if (s3Client == null) {
            throw new IllegalStateException("S3SinkContext not initialized. Call init() first.");
        }

        try {
            // Adapt the content to ensure it's base64 encoded
            String adaptedItem = S3SinkAdapter.adaptContent(item);

            // Parse the adapted item
            String[] parts = adaptedItem.split("\\|", 3);
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid item format");
            }

            String sourceInfo = parts[0];
            String typeInfo = parts[1];
            String base64Content = parts[2];

            // Extract filename from SOURCE=<filename>
            String filename = sourceInfo.substring(sourceInfo.indexOf('=') + 1);
            
            // Create S3 key with prefix
            String s3Key = prefix + filename;

            // Decode base64 content
            byte[] content = java.util.Base64.getDecoder().decode(base64Content);

            // Create metadata
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType(getContentType(typeInfo));
            metadata.setContentLength(content.length);

            // Upload to S3
            PutObjectRequest putRequest = new PutObjectRequest(
                bucketName,
                s3Key,
                new ByteArrayInputStream(content),
                metadata
            );

            s3Client.putObject(putRequest);
            logger.info("Successfully uploaded {} to S3", s3Key);
        } catch (Exception e) {
            logger.error("Failed to upload to S3: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to upload to S3: " + e.getMessage(), e);
        }
    }

    private String getContentType(String typeInfo) {
        // Extract type from TYPE=<type>
        String type = typeInfo.substring(typeInfo.indexOf('=') + 1);
        return switch (type.toUpperCase()) {
            case "CSV" -> "text/csv";
            case "PARQUET" -> "application/x-parquet";
            case "CRC" -> "application/octet-stream";
            case "BINARY" -> "application/octet-stream";
            default -> "application/octet-stream";
        };
    }

    @Override
    public void close() {
        if (s3Client != null) {
            s3Client.shutdown();
            logger.info("S3 client shutdown completed");
        }
    }
} 