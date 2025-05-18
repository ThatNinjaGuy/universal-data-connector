package com.example.pipeline.factory.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.util.Set;
import java.util.HashSet;
import java.util.regex.Pattern;

public class FileSourceContext implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(FileSourceContext.class);
    private final String directory;
    private final Pattern filePattern;
    // Static set to maintain processed files across context instances
    private static final Set<String> processedFiles = new HashSet<>();

    public FileSourceContext(String directory, String pattern) {
        this.directory = directory;
        this.filePattern = convertGlobToRegex(pattern);
        logger.info("Initialized FileSourceContext with directory: {} and pattern: {}", directory, pattern);
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
            File dir = new File(directory);
            if (!dir.exists()) {
                dir.mkdirs();
                logger.info("Created directory: {}", directory);
                return items;
            }

            if (!dir.isDirectory()) {
                logger.error("Path is not a directory: {}", directory);
                return items;
            }

            // List files matching the pattern
            File[] files = dir.listFiles((d, name) -> {
                File f = new File(d, name);
                boolean matches = f.isFile() && matchesPattern(name);
                logger.debug("File {} matches pattern: {}", name, matches);
                return matches;
            });
            
            if (files != null) {
                for (File file : files) {
                    String fileName = file.getName();
                    // Skip if file has already been processed
                    if (!processedFiles.contains(fileName)) {
                        try {
                            // Read entire file content at once
                            String content = Files.readString(file.toPath());
                            
                            // Detect file type
                            String fileType = detectFileType(fileName, content);
                            logger.info("Processing file {} of type {}", fileName, fileType);
                            
                            // Create metadata string with source file info
                            // Format: SOURCE=<filename>|TYPE=<filetype>|<content>
                            String item = String.format("SOURCE=%s|TYPE=%s|%s",
                                fileName,
                                fileType,
                                content);
                            items.add(item);
                            
                            // Move file to processed directory to prevent reprocessing
                            File processedDir = new File("data/processed");
                            if (!processedDir.exists()) {
                                processedDir.mkdirs();
                            }
                            
                            File destFile = new File(processedDir, fileName);
                            if (file.renameTo(destFile)) {
                                processedFiles.add(fileName);
                                logger.info("Successfully processed and moved {} to processed directory", fileName);
                            } else {
                                logger.error("Failed to move file {} to processed directory", fileName);
                            }
                        } catch (IOException e) {
                            logger.error("Failed to read file {}: {}", fileName, e.getMessage());
                        }
                    } else {
                        logger.debug("File {} has already been processed, skipping", fileName);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Failed to read files: {}", e.getMessage(), e);
        }
        return items;
    }
} 