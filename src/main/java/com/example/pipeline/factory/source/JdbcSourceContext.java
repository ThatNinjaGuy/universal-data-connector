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

        logger.info("Initializing JDBC source with query: {} and batch size: {}", query, batchSize);
        connection = DriverManager.getConnection(jdbcUrl, user, password);
        statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        resultSet = statement.executeQuery();
        metaData = resultSet.getMetaData();
        
        columnNames = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            columnNames.add(metaData.getColumnName(i));
        }
        logger.debug("Initialized statement with columns: {}", columnNames);
    }

    public List<String> readBatch() throws SQLException {
        List<String> batch = new ArrayList<>();
        
        // Add headers as first row if this is the first batch
        if (isFirstBatch) {
            StringJoiner headerJoiner = new StringJoiner(",");
            for (String columnName : columnNames) {
                headerJoiner.add(columnName);
            }
            batch.add(headerJoiner.toString());
            isFirstBatch = false;
            logger.debug("Added headers to first batch: {}", columnNames);
        }

        // Read rows
        int count = 0;
        while (count < batchSize && resultSet.next()) {
            StringJoiner rowJoiner = new StringJoiner(",");
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String value = resultSet.getString(i);
                // Handle null values and escape commas
                if (value == null) {
                    value = "";
                } else if (value.contains(",")) {
                    value = "\"" + value.replace("\"", "\"\"") + "\"";
                }
                rowJoiner.add(value);
            }
            batch.add(rowJoiner.toString());
            count++;
        }
        
        logger.debug("Read {} rows in current batch", count);
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