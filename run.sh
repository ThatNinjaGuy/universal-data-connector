#!/bin/bash

# Build the application
./mvnw clean package

# Run the application with the config directory
java -jar target/hazelcast-data-pipeline-1.0-SNAPSHOT.jar src/main/resources 