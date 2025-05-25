package com.example.pipeline.factory.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSinkContext implements SinkContext<String>, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(JdbcSinkContext.class);
    private final Map<String, String> properties;
    private Connection connection;
    private PreparedStatement statement;

    public JdbcSinkContext(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public void init() {
        try {
            this.connection = DriverManager.getConnection(
                properties.get("jdbcUrl"),
                properties.get("user"),
                properties.get("password")
            );
            
            String table = properties.get("table");
            String columns = properties.getOrDefault("columns", "*");
            this.statement = connection.prepareStatement(
                String.format("INSERT INTO %s (%s) VALUES (?)", table, columns)
            );
            
            connection.setAutoCommit(false); // For better performance
            logger.info("Initialized JDBC sink for table: {}", table);
        } catch (SQLException e) {
            logger.error("Failed to initialize JDBC sink: {}", e.getMessage());
            throw new RuntimeException("Failed to initialize JDBC sink: " + e.getMessage(), e);
        }
    }

    @Override
    public void receive(String item) {
        try {
            if (item == null || item.isEmpty()) {
                logger.warn("Received empty item, skipping");
                return;
            }

            statement.setString(1, item);
            statement.executeUpdate();
            
            // Commit every 100 records
            if (statement.getUpdateCount() % 100 == 0) {
                connection.commit();
                logger.debug("Committed batch of 100 records");
            }
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                logger.error("Failed to rollback transaction: {}", rollbackEx.getMessage());
            }
            logger.error("Failed to insert data: {}", e.getMessage());
            throw new RuntimeException("Failed to insert data: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            if (connection != null && !connection.getAutoCommit()) {
                connection.commit(); // Commit any remaining transactions
                logger.info("Committed remaining transactions");
            }
        } catch (SQLException e) {
            logger.error("Failed to commit remaining transactions: {}", e.getMessage());
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
                logger.info("Successfully closed JDBC resources");
            } catch (SQLException e) {
                logger.error("Error closing JDBC resources: {}", e.getMessage());
            }
        }
    }
} 