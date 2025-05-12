#!/bin/bash

# Build the application
./mvnw clean package

# Run the application
java -jar target/hazelcast-data-pipeline.jar pipeline-config.yaml 