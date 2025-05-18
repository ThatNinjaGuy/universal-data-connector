# CSV to PostgreSQL Pipeline Testing Guide

This guide explains how to set up and test the CSV to PostgreSQL pipeline in the Universal Data Connector.

## Prerequisites

### 1. Install PostgreSQL

#### On macOS (using Homebrew)

```bash
# Install Homebrew if not already installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install PostgreSQL
brew install postgresql@15

# Start PostgreSQL service
brew services start postgresql@15
```

#### On Ubuntu/Debian

```bash
# Update package list
sudo apt update

# Install PostgreSQL
sudo apt install postgresql postgresql-contrib

# Start PostgreSQL service
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

#### On Windows

1. Download PostgreSQL installer from [official website](https://www.postgresql.org/download/windows/)
2. Run the installer and follow the setup wizard
3. Keep note of the password you set for the postgres user
4. Ensure the PostgreSQL service is running

### 2. Verify PostgreSQL Installation

```bash
# Check PostgreSQL version
psql --version

# Connect to PostgreSQL
psql -d postgres
```

## Setting Up the Database

1. Create the database and table:

```bash
# Execute the create_table.sql script
psql -f src/main/resources/create_table.sql
```

2. Verify the table creation:

```bash
psql -d udc -c "\d employee"
```

## Preparing Test Data

1. Create the input directory:

```bash
mkdir -p data/input/csv-to-postgres
```

2. Create a test CSV file (test.csv):

```bash
cat > data/input/csv-to-postgres/test.csv << EOL
ID,NAME,EMAIL,DEPARTMENT,SALARY
1,John Doe,john.doe@example.com,Engineering,75000.00
2,Jane Smith,jane.smith@example.com,Marketing,65000.00
3,Bob Johnson,bob.johnson@example.com,Finance,70000.00
EOL
```

## Pipeline Configuration

The pipeline configuration in `src/main/resources/pipeline-config.yaml` should include:

```yaml
- name: "csv-to-postgres"
  source:
    type: file
    properties:
      path: "data/input/csv-to-postgres"
      pattern: "*.csv"
  transformations:
    - type: map
      properties:
        format: "csv"
        columnMapping:
          id: "ID"
          name: "NAME"
          email: "EMAIL"
          department: "DEPARTMENT"
          salary: "SALARY"
  sink:
    type: jdbc
    properties:
      jdbcUrl: "jdbc:postgresql://localhost:5432/udc"
      user: "your_username"
      password: "your_password"
      table: "employee"
      query: "INSERT INTO employee (id, name, email, department, salary) VALUES (?, ?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email, department = EXCLUDED.department, salary = EXCLUDED.salary"
      batchSize: 1000
```

## Running the Pipeline

1. Build the project:

```bash
mvn clean package
```

2. Run the application:

```bash
java -jar target/universal-data-connector-1.0-SNAPSHOT.jar
```

## Verifying Results

1. Check if data was inserted:

```bash
psql -d udc -c "SELECT * FROM employee;"
```

2. Check processed files:

```bash
ls -l data/processed/
```

## Troubleshooting

### Common Issues

1. **Connection Failed**
   - Verify PostgreSQL is running: `brew services list` (macOS) or `systemctl status postgresql` (Linux)
   - Check credentials in pipeline-config.yaml
   - Ensure database 'udc' exists

2. **Permission Denied**
   - Check user permissions: `psql -d udc -c "\du"`
   - Grant necessary permissions: `psql -d udc -c "GRANT ALL PRIVILEGES ON TABLE employee TO your_username;"`

3. **Data Not Appearing**
   - Check input directory for CSV files
   - Verify CSV header names match configuration
   - Check application logs for errors
   - Ensure CSV file has correct format and encoding (UTF-8)

### Logging

To enable detailed logging, add the following to your `application.properties`:

```properties
logging.level.com.example.pipeline=DEBUG
```

## Testing Different Scenarios

1. **Update Existing Records**

```bash
# Create a CSV with updated information
cat > data/input/csv-to-postgres/updates.csv << EOL
ID,NAME,EMAIL,DEPARTMENT,SALARY
1,John Doe Jr,john.jr@example.com,Engineering,80000.00
EOL
```

2. **Handle Special Characters**

```bash
# Test with special characters
cat > data/input/csv-to-postgres/special.csv << EOL
ID,NAME,EMAIL,DEPARTMENT,SALARY
4,"O'Connor, Mary",mary.o@example.com,"R&D",72000.00
EOL
```

3. **Batch Processing**

```bash
# Create a larger dataset
python3 -c "
for i in range(5, 1005):
    print(f'{i},User {i},user{i}@example.com,Department {i%5},${50000+i*100}.00')
" >> data/input/csv-to-postgres/batch.csv
```

## Clean Up

To reset the environment:

```bash
# Clear the database
psql -d udc -c "TRUNCATE TABLE employee;"

# Clear processed files
rm -rf data/processed/*
```

- name: "csv-to-postgres"
    source:
      type: file
      properties:
        path: "data/input/csv-to-postgres"
        pattern: "*.csv"
    transformations:
  - type: map
        properties:
          format: "csv"
          columnMapping:
            id: "ID"
            name: "NAME"
            email: "EMAIL"
            department: "DEPARTMENT"
            salary: "SALARY"
    sink:
      type: jdbc
      properties:
        jdbcUrl: "jdbc:postgresql://localhost:5432/udc"
        user: "deadshot"
        password: ""
        table: "employee"
        query: "INSERT INTO employee (id, name, email, department, salary) VALUES (?, ?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email, department = EXCLUDED.department, salary = EXCLUDED.salary"
        batchSize: 1000

- name: "postgres-to-csv"
    source:
      type: jdbc
      properties:
        jdbcUrl: "jdbc:postgresql://localhost:5432/udc"
        user: "deadshot"
        password: ""
        table: "employee"
        query: "SELECT id, name, email, department, salary FROM employee ORDER BY id"
        batchSize: 1000
        oneTimeOperation: true
    transformations:
  - type: map
        properties:
          format: "csv"
          columnMapping:
            id: "id"
            name: "name"
            email: "email"
            department: "department"
            salary: "salary"
    sink:
      type: file
      properties:
        path: "data/output/postgres-to-csv"
        prefix: "employee_export"
        extension: ".csv"
        includeHeaders: true
