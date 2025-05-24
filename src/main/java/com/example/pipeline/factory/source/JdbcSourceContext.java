package com.example.pipeline.factory.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.io.Serializable;
import java.util.StringJoiner;

public class JdbcSourceContext implements Serializable, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(JdbcSourceContext.class);
    private final Connection connection;
    private final String query;
    private final int batchSize;
    private ResultSet resultSet;
    private PreparedStatement statement;
    private ResultSetMetaData metaData;
    private List<String> columnNames;
    private boolean isFirstBatch = true;

    public JdbcSourceContext(Map<String, String> properties) throws SQLException {
        String jdbcUrl = properties.get("jdbcUrl");
        String user = properties.get("user");
        String password = properties.get("password");
        this.query = properties.getOrDefault("query", "SELECT * FROM " + properties.get("table"));
        this.batchSize = Integer.parseInt(properties.getOrDefault("batchSize", "1000"));

        if (jdbcUrl == null || user == null) {
            throw new SQLException("Required properties 'jdbcUrl' and 'user' must be provided");
        }

        logger.info("Initializing JDBC source with URL: {}, query: {}, batch size: {}", jdbcUrl, query, batchSize);
        
        try {
            connection = DriverManager.getConnection(jdbcUrl, user, password);
            logger.info("Successfully established database connection");
            
            // Validate the connection
            if (!connection.isValid(5)) {
                throw new SQLException("Database connection is invalid");
            }
            
            // Test the query first
            try (Statement testStmt = connection.createStatement()) {
                ResultSet testRs = testStmt.executeQuery("EXPLAIN " + query);
                logger.info("Query plan validation successful");
            } catch (SQLException e) {
                logger.error("Query validation failed: {}", e.getMessage());
                throw new SQLException("Invalid query: " + e.getMessage(), e);
            }
            
            // Get total count
            try (Statement countStmt = connection.createStatement()) {
                ResultSet countRs = countStmt.executeQuery("SELECT COUNT(*) FROM (" + query + ") as temp");
                if (countRs.next()) {
                    long totalRows = countRs.getLong(1);
                    logger.info("Total rows to be fetched: {}", totalRows);
                }
            } catch (SQLException e) {
                logger.error("Failed to get row count: {}", e.getMessage());
                throw new SQLException("Failed to get row count: " + e.getMessage(), e);
            }
            
            statement = connection.prepareStatement(query, 
                ResultSet.TYPE_SCROLL_INSENSITIVE, 
                ResultSet.CONCUR_READ_ONLY);
            resultSet = statement.executeQuery();
            metaData = resultSet.getMetaData();
            
            columnNames = new ArrayList<>();
            StringBuilder columnTypes = new StringBuilder();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                columnNames.add(columnName.toLowerCase());
                columnTypes.append(columnName)
                         .append("(")
                         .append(metaData.getColumnTypeName(i))
                         .append("), ");
            }
            logger.info("Initialized statement with columns and types: {}", columnTypes);
            
            // Position cursor at the beginning
            resultSet.beforeFirst();
            
        } catch (SQLException e) {
            logger.error("Failed to initialize JDBC source: {}", e.getMessage(), e);
            throw e;
        }
    }

    public List<String> readBatch() throws SQLException {
        List<String> batch = new ArrayList<>();
        try {
            StringJoiner csvContent = new StringJoiner("\n");
            
            if (isFirstBatch) {
                // Add headers
                StringJoiner headerJoiner = new StringJoiner(",");
                for (String columnName : columnNames) {
                    headerJoiner.add(columnName);
                }
                csvContent.add(headerJoiner.toString());
                isFirstBatch = false;
            }

            int count = 0;
            while (count < batchSize && resultSet.next()) {
                StringJoiner rowJoiner = new StringJoiner(",");
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String value;
                    int columnType = metaData.getColumnType(i);
                    
                    // Handle different SQL types appropriately
                    if (resultSet.getObject(i) == null) {
                        value = "";
                    } else {
                        switch (columnType) {
                            case Types.TIMESTAMP:
                            case Types.DATE:
                            case Types.TIME:
                                value = resultSet.getObject(i).toString();
                                break;
                            case Types.NUMERIC:
                            case Types.DECIMAL:
                            case Types.DOUBLE:
                            case Types.FLOAT:
                                value = String.valueOf(resultSet.getBigDecimal(i));
                                break;
                            case Types.INTEGER:
                            case Types.BIGINT:
                            case Types.SMALLINT:
                                value = String.valueOf(resultSet.getLong(i));
                                break;
                            case Types.BOOLEAN:
                                value = String.valueOf(resultSet.getBoolean(i));
                                break;
                            default:
                                value = resultSet.getString(i);
                        }
                    }
                    
                    // Handle CSV escaping
                    if (value != null && (value.contains(",") || value.contains("\"") || value.contains("\n"))) {
                        value = "\"" + value.replace("\"", "\"\"") + "\"";
                    }
                    
                    rowJoiner.add(value);
                }
                csvContent.add(rowJoiner.toString());
                count++;
            }
            
            if (count > 0) {
                // Format the output in the expected JDBC_SOURCE format
                String formattedOutput = String.format("JDBC_SOURCE|CSV|%s", csvContent.toString());
                batch.add(formattedOutput);
                logger.info("Emitting JDBC_SOURCE formatted content with {} rows", count);
            }
        } catch (SQLException e) {
            logger.error("Error reading batch: {}", e.getMessage(), e);
            throw e;
        }
        return batch;
    }

    @Override
    public void close() throws Exception {
        if (resultSet != null) {
            resultSet.close();
        }
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
} 