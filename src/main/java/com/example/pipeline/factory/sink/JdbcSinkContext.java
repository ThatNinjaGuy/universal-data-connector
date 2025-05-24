package com.example.pipeline.factory.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

public class JdbcSinkContext implements AutoCloseable {
    private final Connection connection;
    private final PreparedStatement statement;

    public JdbcSinkContext(Map<String, String> properties) throws SQLException {
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
        } catch (SQLException e) {
            throw new SQLException("Failed to initialize JDBC sink: " + e.getMessage(), e);
        }
    }

    public void receive(String item) throws SQLException {
        try {
            statement.setString(1, item);
            statement.executeUpdate();
            
            // Commit every 100 records
            if (statement.getUpdateCount() % 100 == 0) {
                connection.commit();
            }
        } catch (SQLException e) {
            connection.rollback();
            throw new SQLException("Failed to insert data: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            if (connection != null && !connection.getAutoCommit()) {
                connection.commit(); // Commit any remaining transactions
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
} 