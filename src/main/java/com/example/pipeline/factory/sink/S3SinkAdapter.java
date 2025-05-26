package com.example.pipeline.factory.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Base64;

public class S3SinkAdapter {
    private static final Logger logger = LoggerFactory.getLogger(S3SinkAdapter.class);

    public static String adaptContent(String item) {
        try {
            // Parse the item which contains metadata and content
            // Format: SOURCE=<filename>|TYPE=<filetype>|<content>
            String[] parts = item.split("\\|", 3);
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid item format");
            }

            String sourceInfo = parts[0];
            String typeInfo = parts[1];
            String content = parts[2];

            // Check if content is already base64 encoded
            if (isBase64Encoded(content)) {
                return item; // Already encoded, return as is
            }

            // Encode content to base64
            String base64Content = Base64.getEncoder().encodeToString(content.getBytes());
            
            // Reconstruct the item with base64 encoded content
            return String.format("%s|%s|%s", sourceInfo, typeInfo, base64Content);
        } catch (Exception e) {
            logger.error("Failed to adapt content: {}", e.getMessage());
            throw new RuntimeException("Failed to adapt content: " + e.getMessage(), e);
        }
    }

    private static boolean isBase64Encoded(String content) {
        try {
            // Try to decode the content
            Base64.getDecoder().decode(content);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
} 