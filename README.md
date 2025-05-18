# Universal Data Connector

A flexible and extensible data pipeline built with Hazelcast Jet that supports various data sources and sinks with configurable transformations. The application uses a factory pattern to create and manage different types of data sources and sinks, making it easy to add new connectors.

## Architecture

The application follows a factory-based architecture:

- `SourceFactory`: Creates and manages different types of data sources
  - Kafka Source: Streams data from Kafka topics
  - File Source: Watches directories for new files
  - JDBC Source: Reads data from databases

- `SinkFactory`: Creates and manages different types of data sinks
  - Kafka Sink: Writes data to Kafka topics
  - File Sink: Writes data to files (text, CSV, Parquet)
  - JDBC Sink: Writes data to databases

## Prerequisites

- Java 17 or higher
- Maven 3.6 or higher
- Apache Kafka 3.6.x (for Kafka source/sink)

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

## Setting up Kafka (if using Kafka source/sink)

### For Mac Users (using Homebrew)

1. Install Kafka using Homebrew (this will also install Zookeeper):

```bash
# Install Homebrew if you haven't already
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Kafka
brew install kafka
```

2. Start Zookeeper (in a separate terminal):

```bash
brew services start zookeeper
```

3. Start Kafka (in a separate terminal):

```bash
brew services start kafka
```

4. Verify services are running:

```bash
brew services list
# Should show both kafka and zookeeper as "started"
```

### For Other Operating Systems (Manual Setup)

1. Download and extract Kafka:

```bash
wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz
tar -xzf kafka_2.13-3.6.1.tgz
cd kafka_2.13-3.6.1
```

2. Start Zookeeper (in a separate terminal):

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

3. Start Kafka Server (in a separate terminal):

```bash
bin/kafka-server-start.sh config/server.properties
```

### Creating Required Topics

1. Create the topics needed for the pipeline:

```bash
# Create input topic
kafka-topics --create --topic input-topic \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1

# Create output topic
kafka-topics --create --topic output-topic \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1
```

2. Verify topics were created:

```bash
kafka-topics --list --bootstrap-server localhost:9092
```

### Stopping Kafka (when done)

For Mac users:

```bash
# Stop Kafka
brew services stop kafka

# Stop Zookeeper
brew services stop zookeeper
```

For other operating systems:

```bash
# Stop Kafka
bin/kafka-server-stop.sh

# Stop Zookeeper
bin/zookeeper-server-stop.sh
```

### Troubleshooting Kafka

1. Verify Kafka is running:

```bash
lsof -i :9092
```

2. Check Kafka logs:

```bash
# For Mac (Homebrew installation)
tail -f /usr/local/var/log/kafka/kafka_output.log

# For manual installation
tail -f logs/server.log
```

3. Check Zookeeper logs:

```bash
# For Mac (Homebrew installation)
tail -f /usr/local/var/log/zookeeper/zookeeper.log

# For manual installation
tail -f logs/zookeeper.log
```

## Supported Data Sources

### Kafka Source

- Supports multiple topics
- Configurable consumer groups
- Customizable deserializers
- Offset management

### File Source

- Directory monitoring
- File pattern matching
- Automatic file type detection (CSV, TEXT)
- Batch processing support

### JDBC Source

- Database connectivity
- SQL query support
- Batch fetching
- Connection pooling

## Supported Data Sinks

### Kafka Sink

- Multiple topic support
- Customizable serializers
- Producer configuration
- Error handling

### File Sink

- Multiple output formats (text, CSV, Parquet)
- Configurable file naming
- Header management
- Batch writing

### JDBC Sink

- Database connectivity
- Batch inserts
- Transaction management
- Error handling

## Configuration

The pipeline is configured using YAML. Example configuration in `src/main/resources/pipeline-config.yaml`:

```yaml
source:
  type: kafka  # or file, jdbc
  properties:
    # Kafka specific properties
    bootstrapServers: localhost:9092
    topic: input-topic
    groupId: my-group
    autoOffsetReset: earliest
    keyDeserializer: org.apache.kafka.common.serialization.StringDeserializer
    valueDeserializer: org.apache.kafka.common.serialization.StringDeserializer

    # File specific properties
    directory: /path/to/watch
    pattern: "*.csv"
    
    # JDBC specific properties
    url: jdbc:postgresql://localhost:5432/mydb
    username: user
    password: pass
    query: "SELECT * FROM mytable"

transformations:
  - type: filter
    properties:
      condition: "important"
  - type: map
    properties:
      prefix: "processed-"
      suffix: "-done"

sink:
  type: kafka  # or file, jdbc
  properties:
    # Kafka specific properties
    bootstrapServers: localhost:9092
    topic: output-topic
    keySerializer: org.apache.kafka.common.serialization.StringSerializer
    valueSerializer: org.apache.kafka.common.serialization.StringSerializer
    
    # File specific properties
    path: /path/to/output
    format: parquet  # or text, csv
    prefix: output
    extension: .parquet
    
    # JDBC specific properties
    url: jdbc:postgresql://localhost:5432/mydb
    username: user
    password: pass
    table: output_table
```

## Running the Application

1. Start the pipeline:

```bash
java -jar target/hazelcast-data-pipeline-1.0-SNAPSHOT.jar
```

## Testing

1. Produce test messages to input topic:

```bash
bin/kafka-console-producer.sh --topic input-topic \
    --bootstrap-server localhost:9092
```

Then type messages like:

```
this is important message
this is not filtered
another important update
```

2. Consume processed messages from output topic:

```bash
bin/kafka-console-consumer.sh --topic output-topic \
    --from-beginning \
    --bootstrap-server localhost:9092
```

You should see filtered and transformed messages like:

```
processed-this is important message-done
processed-another important update-done
```

## Supported Sources

- Kafka
- File (file watcher)

## Supported Sinks

- Kafka
- File
- JDBC

## Supported Transformations

- Filter: Filters messages based on a condition
- Map: Transforms messages by adding prefix/suffix

## Troubleshooting

1. If you see "No resolvable bootstrap urls":
   - Ensure Kafka is running
   - Verify bootstrap server configuration
   - Check network connectivity

2. If messages aren't being processed:
   - Verify topics exist
   - Check source/sink configuration
   - Review transformation conditions

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

### Adding New Connectors

To add a new connector:

1. Create a new source/sink class in the appropriate factory package
2. Implement the required interfaces and methods
3. Add the new connector type to the factory class
4. Update the configuration validation
5. Add appropriate tests
6. Update documentation

## License

[Add your license here]
