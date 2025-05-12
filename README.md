# Universal Data Connector

A flexible data pipeline built with Hazelcast Jet that supports various sources and sinks with configurable transformations.

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

4. Create required topics:

```bash
# Create input topic
bin/kafka-topics.sh --create --topic input-topic \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1

# Create output topic
bin/kafka-topics.sh --create --topic output-topic \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1
```

## Configuration

The pipeline is configured using YAML. Example configuration in `src/main/resources/pipeline-config.yaml`:

```yaml
source:
  type: kafka
  properties:
    bootstrapServers: localhost:9092
    topic: input-topic
    groupId: my-group
    autoOffsetReset: earliest
    keyDeserializer: org.apache.kafka.common.serialization.StringDeserializer
    valueDeserializer: org.apache.kafka.common.serialization.StringDeserializer

transformations:
  - type: filter
    properties:
      condition: "important"
  - type: map
    properties:
      prefix: "processed-"
      suffix: "-done"

sink:
  type: kafka
  properties:
    bootstrapServers: localhost:9092
    topic: output-topic
    keySerializer: org.apache.kafka.common.serialization.StringSerializer
    valueSerializer: org.apache.kafka.common.serialization.StringSerializer
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

## License

[Add your license here]
