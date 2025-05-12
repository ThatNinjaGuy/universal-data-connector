<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" class="logo" width="120"/>

# i want to start implementing these steps using cursor. tell me how to provide instrcutions thought by you as cursor input, so it implements the complete project. we will talk about setup in cursor later. but can you create a document which can be used by cursor ai to implement thr project from scratch.

Absolutely! Here’s a **step-by-step, instruction-oriented document** you can use as input for Cursor AI (or similar AI coding tools) to implement your Hazelcast-based, configuration-driven data sourcing pipeline. This document is structured to be clear, modular, and actionable-perfect for AI-driven code generation.

---

# Hazelcast Data Sourcing Pipeline: Implementation Instructions for Cursor AI

## Project Overview

**Goal:**
Build a configurable data sourcing pipeline using Hazelcast, supporting pluggable sources and sinks, driven by external configuration. The design must be decoupled, enabling easy extension to new connectors and formats.

---

## 1. Project Structure

**Instructions:**

- Create a new Maven or Gradle Java project named `hazelcast-data-pipeline`.
- Organize the project as follows:

```
src/
  main/
    java/
      com.example.pipeline/
        config/
        factory/
        pipeline/
        Main.java
    resources/
      pipeline-config.yaml
```


---

## 2. Define Configuration Model

**Instructions:**

- Use YAML for pipeline configuration.
- Define Java POJOs to map the configuration:
    - `PipelineConfig` (root)
    - `SourceConfig`
    - `SinkConfig`
    - `TransformConfig` (optional, for future extensibility)

**Sample YAML (pipeline-config.yaml):**

```yaml
source:
  type: kafka
  properties:
    bootstrapServers: "localhost:9092"
    topic: "input-topic"
    groupId: "my-group"

sink:
  type: jdbc
  properties:
    jdbcUrl: "jdbc:mysql://localhost:3306/mydb"
    user: "user"
    password: "password"
    table: "output_table"
```

**Tasks:**

- Use Jackson or SnakeYAML to parse the YAML into Java objects.

---

## 3. Implement Source and Sink Factories

**Instructions:**

- In `factory/`, create:
    - `SourceFactory.java`
    - `SinkFactory.java`
- Each factory should have a static `create` method that takes the respective config and returns a Hazelcast `Source` or `Sink` object.
- Use a `switch` or registry pattern to support multiple types (e.g., kafka, jdbc, mongodb, s3, etc.).
- For now, implement at least Kafka (source) and JDBC (sink). Add stubs for others.

**Example (SourceFactory.java):**

```java
public class SourceFactory {
    public static Source<?> create(SourceConfig config) {
        switch (config.getType().toLowerCase()) {
            case "kafka":
                // Return Hazelcast Kafka source
            // Add more cases as needed
            default:
                throw new IllegalArgumentException("Unknown source type: " + config.getType());
        }
    }
}
```


---

## 4. Pipeline Builder

**Instructions:**

- In `pipeline/`, create `PipelineBuilder.java`.
- Implement a method that:
    - Accepts a `PipelineConfig`.
    - Uses the factories to create source and sink.
    - Optionally applies transforms (future extensibility).
    - Builds and returns a Hazelcast `Pipeline` object.

**Example:**

```java
public class PipelineBuilder {
    public static Pipeline build(PipelineConfig config) {
        Pipeline pipeline = Pipeline.create();
        StreamStage<?> stage = pipeline.readFrom(SourceFactory.create(config.getSource()));
        // Future: Apply transforms here
        stage.writeTo(SinkFactory.create(config.getSink()));
        return pipeline;
    }
}
```


---

## 5. Main Application

**Instructions:**

- In `Main.java`:
    - Load the configuration YAML.
    - Build the pipeline using `PipelineBuilder`.
    - Start a Hazelcast instance (embedded mode for now).
    - Submit the pipeline as a Jet job.

**Example:**

```java
public class Main {
    public static void main(String[] args) {
        // Load YAML config
        // Build pipeline
        // Start Hazelcast instance
        // Submit pipeline job
    }
}
```


---

## 6. Extensibility

**Instructions:**

- Ensure the factories can be easily extended to support new source/sink types by adding new cases or using a plugin/registry approach.
- Document how to add a new connector (e.g., MongoDB, S3).

---

## 7. Testing

**Instructions:**

- Add unit tests for:
    - Configuration parsing
    - Factory creation logic
- Add a sample integration test that runs the pipeline with mock data.

---

## 8. Documentation

**Instructions:**

- Write a `README.md` with:
    - Project purpose
    - How to configure sources/sinks
    - How to run the pipeline
    - How to add new connectors

---

## 9. Next Steps (Future)

**Instructions:**

- Plan to add:
    - Transformation steps (mapping, filtering, enrichment)
    - Support for more connectors
    - Dynamic configuration reloads
    - Deployment scripts for Kubernetes

---

# End of Instructions


---

**How to use with Cursor AI:**

- Paste each section as a prompt, or upload the whole document.
- For each step, ask Cursor AI to generate the code, config, or documentation as specified.
- Review and iterate on the generated code, ensuring it aligns with the design and is clean, modular, and extensible.

---

Let me know if you want the first code snippets or want to discuss the setup in Cursor!

