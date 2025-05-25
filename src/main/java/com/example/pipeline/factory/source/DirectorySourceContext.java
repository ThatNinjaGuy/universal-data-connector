package com.example.pipeline.factory.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.Base64;

public class DirectorySourceContext implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(DirectorySourceContext.class);
    private final String baseDirectory;
    private final Pattern filePattern;
    private final boolean recursive;
    private final boolean preserveStructure;
    // Static set to maintain processed files across context instances
    private static final Set<String> processedFiles = new HashSet<>();

    public DirectorySourceContext(String baseDirectory, String pattern, boolean recursive, boolean preserveStructure) {
        this.baseDirectory = baseDirectory;
        this.filePattern = convertGlobToRegex(pattern);
        this.recursive = recursive;
        this.preserveStructure = preserveStructure;
        logger.info("Initialized DirectorySourceContext with base directory: {}, pattern: {}, recursive: {}", 
            baseDirectory, pattern, recursive);
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

    private String detectFileType(String filename) {
        String lowerFilename = filename.toLowerCase();
        if (lowerFilename.endsWith(".csv")) {
            return "CSV";
        } else if (lowerFilename.endsWith(".parquet")) {
            return "PARQUET";
        } else if (lowerFilename.endsWith(".crc")) {
            return "CRC";
        }
        return "BINARY";
    }

    private String getRelativePath(Path file) {
        Path basePath = Paths.get(baseDirectory);
        return basePath.relativize(file).toString();
    }

    public List<String> readNewFiles() {
        List<String> items = new ArrayList<>();
        try {
            Path basePath = Paths.get(baseDirectory);
            if (!Files.exists(basePath)) {
                Files.createDirectories(basePath);
                logger.info("Created base directory: {}", baseDirectory);
                return items;
            }

            Files.walkFileTree(basePath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    try {
                        if (!attrs.isRegularFile()) {
                            return FileVisitResult.CONTINUE;
                        }

                        String filename = file.getFileName().toString();
                        if (!matchesPattern(filename)) {
                            return FileVisitResult.CONTINUE;
                        }

                        String relativePath = getRelativePath(file);
                        if (processedFiles.contains(relativePath)) {
                            logger.debug("File {} has already been processed, skipping", relativePath);
                            return FileVisitResult.CONTINUE;
                        }

                        // Read file content as bytes
                        byte[] content = Files.readAllBytes(file);
                        String fileType = detectFileType(filename);
                        
                        // Convert bytes to base64 for safe transmission
                        String base64Content = Base64.getEncoder().encodeToString(content);
                        
                        // Create metadata string with source file info
                        String item = String.format("SOURCE=%s|TYPE=%s|%s",
                            relativePath,
                            fileType,
                            base64Content);
                        items.add(item);
                        
                        processedFiles.add(relativePath);
                        logger.info("Successfully processed file {}", relativePath);
                    } catch (IOException e) {
                        logger.error("Failed to process file {}: {}", file, e.getMessage());
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    if (!recursive && !dir.equals(basePath)) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            logger.error("Failed to scan directory: {}", e.getMessage(), e);
        }
        return items;
    }
} 