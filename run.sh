#!/bin/bash

# Build the application
./mvnw clean install

# Run the application with the config directory
java -jar target/hazelcast-data-pipeline-1.0-SNAPSHOT.jar --server.port=8081 src/main/resources