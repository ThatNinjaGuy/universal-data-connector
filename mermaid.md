graph TD
    A[Configuration Files] --> B[PipelineConfig]
    B --> C[PipelineBuilder]
    C --> D[SourceFactory]
    C --> E[SinkFactory]
    D --> F[Pipeline Processing]
    E --> F
    F --> G[Data Transformations]
    G --> H[Output]
    
    subgraph "Transformation Layer"
    G1[SerializableFilter] --> G
    G2[SerializableMapper] --> G
    end
