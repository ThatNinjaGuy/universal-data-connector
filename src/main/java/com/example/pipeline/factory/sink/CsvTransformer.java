package com.example.pipeline.factory.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class CsvTransformer {
    private static final Logger logger = LoggerFactory.getLogger(CsvTransformer.class);
    private final Map<String, String> columnMapping;
    private final boolean includeHeaders;

    public CsvTransformer(Map<String, String> columnMapping, boolean includeHeaders) {
        this.columnMapping = columnMapping;
        this.includeHeaders = includeHeaders;
        logger.debug("Initialized CsvTransformer with columnMapping: {}, includeHeaders: {}", 
            columnMapping, includeHeaders);
    }

    public String transform(String csvContent) {
        try {
            String[] lines = csvContent.split("\n", -1);
            if (lines.length < 2) {
                logger.warn("CSV content has insufficient lines: {}", lines.length);
                return csvContent;
            }

            // Process headers
            String[] headers = lines[0].split(",", -1);
            Map<Integer, Integer> columnIndexMap = new HashMap<>();
            List<String> newHeaders = new ArrayList<>();

            // Create mapping from source columns to target columns
            for (Map.Entry<String, String> entry : columnMapping.entrySet()) {
                String targetColumn = entry.getKey();
                String sourceColumn = entry.getValue();

                // Find source column index
                for (int i = 0; i < headers.length; i++) {
                    if (headers[i].trim().equalsIgnoreCase(sourceColumn)) {
                        columnIndexMap.put(i, newHeaders.size());
                        newHeaders.add(targetColumn);
                        break;
                    }
                }
            }

            if (columnIndexMap.isEmpty()) {
                logger.warn("No matching columns found in mapping");
                return csvContent;
            }

            // Build new CSV content
            StringBuilder newContent = new StringBuilder();

            // Add new headers if required
            if (includeHeaders) {
                newContent.append(String.join(",", newHeaders)).append("\n");
            }

            // Process data rows
            for (int i = 1; i < lines.length; i++) {
                String[] fields = lines[i].split(",", -1);
                String[] newFields = new String[newHeaders.size()];

                // Map fields to new positions
                for (Map.Entry<Integer, Integer> entry : columnIndexMap.entrySet()) {
                    int sourceIndex = entry.getKey();
                    int targetIndex = entry.getValue();

                    if (sourceIndex < fields.length) {
                        newFields[targetIndex] = fields[sourceIndex].trim();
                    } else {
                        newFields[targetIndex] = "";
                    }
                }

                newContent.append(String.join(",", newFields)).append("\n");
            }

            return newContent.toString();
        } catch (Exception e) {
            logger.error("Error transforming CSV content: {}", e.getMessage());
            return csvContent;
        }
    }
} 