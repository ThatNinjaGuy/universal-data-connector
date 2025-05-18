# Universal Data Connector

A flexible and extensible data pipeline built with Hazelcast Jet that supports various data sources and sinks with configurable transformations. The application uses a factory pattern to create and manage different types of data sources and sinks, making it easy to add new connectors.

## Prerequisites

- Java 17 or higher
- Maven 3.6 or higher
- PostgreSQL (for JDBC source/sink functionality)

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd universal-data-connector
```

2. Build the project:

```bash
mvn clean package
```

## Running the Application

1. Start the application:

```bash
./run.sh
```

## Testing the Pipelines

The application supports several data transformation pipelines. To test them, you'll need to set up the input data in the appropriate directories:

### 1. Text to Text Pipeline

1. Create a text file in `data/input/text-to-text/`:

```bash
echo "This is a test message" > data/input/text-to-text/test.txt
```

### 2. CSV to CSV Pipeline

1. Create a CSV file in `data/input/csv-to-csv/`:

```bash
echo "id,name,email
1,John Doe,john@example.com
2,Jane Smith,jane@example.com" > data/input/csv-to-csv/test.csv
```

### 3. CSV to Parquet Pipeline

1. Create a CSV file in `data/input/csv-to-parquet/`:

```bash
echo "id,project,status,assignee,deadline
1,Project A,important,John Doe,2024-12-31
2,Project B,normal,Jane Smith,2024-12-31" > data/input/csv-to-parquet/test.csv
```

### 4. PostgreSQL to Parquet Pipeline

1. Create the employee table in PostgreSQL:

```sql
CREATE TABLE employee (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(10,2)
);

-- Insert sample data
INSERT INTO employee (name, email, department, salary) VALUES
    ('John Doe', 'john.doe@example.com', 'IT', 75000.00),
    ('Jane Smith', 'jane.smith@example.com', 'HR', 65000.00),
    ('Bob Wilson', 'bob.wilson@example.com', 'Sales', 80000.00);
```

## Supported Pipelines

### 1. Text to Text Pipeline

- Source: Text files from `data/input/text-to-text/`
- Sink: Text files to `data/output/text-to-text/`
- Features:
  - Preserves original file names
  - Processes text files in real-time

### 2. CSV to CSV Pipeline

- Source: CSV files from `data/input/csv-to-csv/`
- Sink: CSV files to `data/output/csv-to-csv/`
- Features:
  - Preserves headers
  - Maintains original file names
  - Handles CSV escaping

### 3. CSV to Parquet Pipeline

- Source: CSV files from `data/input/csv-to-parquet/`
- Sink: Parquet files to `data/output/csv-to-parquet/`
- Features:
  - Filters records based on status
  - Converts to Parquet format
  - Supports custom schema
  - Batch processing

### 4. PostgreSQL to Parquet Pipeline

- Source: PostgreSQL database
- Sink: Parquet files to `data/output/postgres-to-parquet/`
- Features:
  - Direct database connection
  - Custom SQL queries
  - Batch processing
  - Schema validation
  - Automatic type conversion

## Configuration

The pipeline is configured using YAML. Example configuration in `src/main/resources/pipeline-config.yaml`:

```yaml
pipelines:
  - name: "text-processor"
    source:
      type: file
      properties:
        path: "data/input/text-to-text"
        pattern: "*.txt"
    sink:
      type: file
      properties:
        path: "data/output/text-to-text"
        extension: ".txt"

  - name: "csv-to-csv"
    source:
      type: file
      properties:
        path: "data/input/csv-to-csv"
        pattern: "*.csv"
    sink:
      type: file
      properties:
        path: "data/output/csv-to-csv"
        extension: ".csv"
        includeHeaders: true

  - name: "csv-to-parquet"
    source:
      type: file
      properties:
        path: "data/input/csv-to-parquet"
        pattern: "*.csv"
    transformations:
      - type: filter
        properties:
          column: "status"
          condition: "important"
    sink:
      type: file
      properties:
        path: "data/output/csv-to-parquet"
        format: "parquet"
        schema: |
          {
            "type": "record",
            "name": "CsvRecord",
            "namespace": "com.example",
            "fields": [
              {"name": "id", "type": "string"},
              {"name": "project", "type": "string"},
              {"name": "status", "type": "string"},
              {"name": "assignee", "type": "string"},
              {"name": "deadline", "type": "string"}
            ]
          }

  - name: "postgres-to-parquet"
    source:
      type: jdbc
      properties:
        jdbcUrl: "jdbc:postgresql://localhost:5432/udc"
        user: "your_username"
        password: "your_password"
        table: "employee"
        query: "SELECT id, name, email, department, salary FROM employee ORDER BY id"
        batchSize: 1000
    sink:
      type: file
      properties:
        path: "data/output/postgres-to-parquet"
        format: "parquet"
        schema: |
          {
            "type": "record",
            "name": "Employee",
            "namespace": "com.example",
            "fields": [
              {"name": "id", "type": "int"},
              {"name": "name", "type": "string"},
              {"name": "email", "type": "string"},
              {"name": "department", "type": "string"},
              {"name": "salary", "type": "double"}
            ]
          }
```

## Troubleshooting

1. If you see database connection errors:
   - Verify PostgreSQL is running
   - Check database credentials in pipeline-config.yaml
   - Ensure the database and table exist

2. If files aren't being processed:
   - Check input directory paths
   - Verify file permissions
   - Ensure file patterns match

3. If Parquet conversion fails:
   - Verify schema matches data structure
   - Check for data type mismatches
   - Ensure all required fields are present

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Add your license here]
