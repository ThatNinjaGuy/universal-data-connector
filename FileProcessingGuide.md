# File Processing Guide

This guide explains how to test the file watcher source and file sink functionality of the Universal Data Connector.

## Directory Structure

```
universal-data-connector/
├── data/
│   ├── input/         # Place input files here
│   ├── output/        # Processed files appear here
│   └── processed/     # Successfully processed input files are moved here
```

## Testing Steps

1. **Setup Directories**

   ```bash
   mkdir -p data/input data/output data/processed
   ```

2. **Create Test Input Files**

   ```bash
   # Create a test file
   echo "important message from file 1
   not so important message
   another important update from file
   this should be filtered out
   very important notification" > data/input/test1.txt

   # Create another test file
   echo "important alert from system
   regular message here
   very important update" > data/input/test2.txt
   ```

3. **Configure Pipeline**

   Ensure your `pipeline-config.yaml` has the following configuration:

   ```yaml
   source:
     type: file
     properties:
       path: "data/input"

   transformations:
     - type: filter
       properties:
         condition: "important"
     - type: map
       properties:
         prefix: "processed-"
         suffix: "-done"

   sink:
     type: file
     properties:
       path: "data/output"
       prefix: "processed"
       extension: ".txt"
   ```

4. **Run the Application**

   ```bash
   mvn clean package
   java -jar target/hazelcast-data-pipeline-1.0-SNAPSHOT.jar
   ```

## What to Expect

1. **Input Processing**
   - The application watches the `data/input` directory
   - When a file is detected, it's processed line by line
   - Files containing "important" are filtered and transformed
   - After processing, input files are moved to `data/processed`

2. **Output Generation**
   - Processed files appear in `data/output`
   - Output files are named: `processed_[input-filename]_[timestamp].txt`
   - Each output file contains only the processed lines from its corresponding input file
   - Lines are prefixed with "processed-" and suffixed with "-done"

3. **Example Output**
   For `test1.txt`, you'll see something like:

   ```
   processed-important message from file 1-done
   processed-another important update from file-done
   processed-very important notification-done
   ```

## Testing Real-time Processing

1. **Add New Files While Running**

   ```bash
   # Create a new file while the application is running
   echo "important real-time test
   another important message" > data/input/test3.txt
   ```

2. **Monitor Results**
   - Watch the `data/output` directory for new processed files
   - Check `data/processed` to see moved input files
   - Review application logs for processing information

## Troubleshooting

1. **No Files Processed**
   - Verify input files are in `data/input`
   - Check file permissions
   - Ensure file content contains "important" for filter condition

2. **Files Not Moving to Processed**
   - Check directory permissions
   - Verify no file locks/handles are open

3. **Output Files Not Created**
   - Check `data/output` directory permissions
   - Verify sink configuration in `pipeline-config.yaml`

## Notes

- Each input file generates its own output file with timestamp
- Files are processed only once
- The system handles multiple files concurrently
- Input files are automatically moved after processing

## CSV File Processing

### CSV Processing Features

- Supports CSV files with headers
- Maintains CSV structure in output
- Allows filtering based on column values
- Preserves column ordering

### Testing with CSV Files

1. **Create Sample CSV Files**

   ```bash
   # Create a test CSV file
   echo "id,name,status,priority
   1,Task A,important,high
   2,Task B,normal,low
   3,Task C,important,medium
   4,Task D,critical,high" > data/input/tasks.csv

   # Create another CSV file
   echo "id,project,status,assignee
   101,Project X,important,John
   102,Project Y,pending,Alice
   103,Project Z,important,Bob" > data/input/projects.csv
   ```

2. **CSV-specific Configuration**

   Modify `pipeline-config.yaml` for CSV processing:

   ```yaml
   source:
     type: file
     properties:
       path: "data/input"
       pattern: "*.csv"    # Only process CSV files

   transformations:
     - type: filter
       properties:
         column: "status"  # Specify column name for filtering
         condition: "important"
     - type: map
       properties:
         columns: ["id", "name", "status"]  # Specify columns to include
         addColumns:
           processed_time: "timestamp"

   sink:
     type: file
     properties:
       path: "data/output"
       prefix: "processed"
       extension: ".csv"
       includeHeaders: true
   ```

3. **Expected CSV Output**

   For `tasks.csv`, the output will look like:

   ```csv
   id,name,status,priority,processed_time
   1,Task A,important,high,2024-05-14T12:34:56
   3,Task C,important,medium,2024-05-14T12:34:56
   ```

### CSV Processing Features to Implement

1. **Header Management**
   - Automatic header detection
   - Header preservation or modification
   - Column selection and reordering

2. **Data Transformations**
   - Type conversion (string to number, date formatting)
   - Column value modifications
   - Adding computed columns

3. **CSV-specific Options**
   - Custom delimiters
   - Quote character handling
   - Escape character handling
   - Multi-line field support

4. **Validation**
   - Schema validation
   - Data type validation
   - Required field checking
   - Custom validation rules

### Implementation Notes

To implement CSV processing, you would need to:

1. Add CSV parsing library (e.g., Apache Commons CSV):

   ```xml
   <dependency>
       <groupId>org.apache.commons</groupId>
       <artifactId>commons-csv</artifactId>
       <version>1.10.0</version>
   </dependency>
   ```

2. Enhance Source and Sink factories to handle CSV:
   - Parse CSV headers
   - Process rows as records
   - Maintain CSV structure
   - Handle CSV-specific configurations

3. Add CSV-specific transformations:
   - Column-based filtering
   - Row transformations
   - Header modifications
   - Data type conversions

4. Implement CSV validation:
   - Schema validation
   - Data type checking
   - Custom validation rules
