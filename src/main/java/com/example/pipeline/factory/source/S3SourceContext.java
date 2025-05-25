package com.example.pipeline.factory.source;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class S3SourceContext implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(S3SourceContext.class);
    private final AmazonS3 s3Client;
    private final String bucketName;
    private final String prefix;
    private final Pattern filePattern;
    // Static set to maintain processed files across context instances
    private static final Set<String> processedFiles = new HashSet<>();

    public S3SourceContext(String bucketName, String region, String accessKey, String secretKey, String prefix, String pattern) {
        try {
            // Initialize AWS credentials
            BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);

            // Create S3 client
            this.s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();

            this.bucketName = bucketName;
            this.prefix = prefix;
            this.filePattern = convertGlobToRegex(pattern);

            // Verify bucket exists
            if (!s3Client.doesBucketExistV2(bucketName)) {
                throw new IllegalStateException("Bucket " + bucketName + " does not exist");
            }

            logger.info("Initialized S3SourceContext for bucket: {}", bucketName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize S3 source: " + e.getMessage(), e);
        }
    }

    private static Pattern convertGlobToRegex(String glob) {
        if (glob == null || glob.isEmpty()) {
            return Pattern.compile(".*");
        }
        StringBuilder regex = new StringBuilder();
        regex.append("^");
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            switch (c) {
                case '*' -> regex.append(".*");
                case '?' -> regex.append(".");
                case '.' -> regex.append("\\.");
                case '\\' -> {
                    regex.append("\\\\");
                    if (i + 1 < glob.length()) {
                        regex.append(glob.charAt(++i));
                    }
                }
                default -> regex.append(c);
            }
        }
        regex.append("$");
        return Pattern.compile(regex.toString(), Pattern.CASE_INSENSITIVE);
    }

    private boolean matchesPattern(String filename) {
        return filePattern.matcher(filename).matches();
    }

    private String detectFileType(String filename, String content) {
        if (filename.toLowerCase().endsWith(".csv")) {
            // Additional CSV validation
            String[] lines = content.split("\n", 2);
            if (lines.length > 0 && lines[0].contains(",")) {
                return "CSV";
            }
        }
        return "TEXT";
    }

    public List<String> readNewFiles() {
        List<String> items = new ArrayList<>();
        try {
            // List objects in the bucket with the specified prefix
            ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(prefix);

            ListObjectsV2Result result;
            do {
                result = s3Client.listObjectsV2(request);
                
                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                    String key = objectSummary.getKey();
                    String filename = key.substring(key.lastIndexOf('/') + 1);
                    
                    // Skip if file has already been processed
                    if (!processedFiles.contains(key) && matchesPattern(filename)) {
                        try {
                            // Read file content
                            S3Object s3Object = s3Client.getObject(bucketName, key);
                            BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
                            StringBuilder content = new StringBuilder();
                            String line;
                            while ((line = reader.readLine()) != null) {
                                content.append(line).append("\n");
                            }
                            reader.close();
                            
                            // Detect file type
                            String fileType = detectFileType(filename, content.toString());
                            logger.info("Processing file {} of type {}", filename, fileType);
                            
                            // Create metadata string with source file info
                            // Format: SOURCE=<filename>|TYPE=<filetype>|<content>
                            String item = String.format("SOURCE=%s|TYPE=%s|%s",
                                filename,
                                fileType,
                                content.toString());
                            items.add(item);
                            
                            processedFiles.add(key);
                            logger.info("Successfully processed file {}", key);
                        } catch (Exception e) {
                            logger.error("Failed to read file {}: {}", key, e.getMessage());
                        }
                    } else {
                        // logger.debug("File {} has already been processed or doesn't match pattern, skipping", key);
                    }
                }
                
                request.setContinuationToken(result.getNextContinuationToken());
            } while (result.isTruncated());
            
        } catch (Exception e) {
            logger.error("Failed to list objects in bucket: {}", e.getMessage(), e);
        }
        return items;
    }
} 