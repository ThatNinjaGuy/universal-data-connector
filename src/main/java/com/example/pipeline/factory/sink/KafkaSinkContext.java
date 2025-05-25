package com.example.pipeline.factory.sink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;
import java.util.Map;

public class KafkaSinkContext implements SinkContext<String>, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSinkContext.class);
    private final Properties props;
    private final String topic;
    private transient KafkaProducer<String, String> producer;

    public KafkaSinkContext(Map<String, String> properties) {
        this.props = new Properties();
        
        // Set bootstrap servers with correct property name
        this.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get("bootstrapServers"));
        this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        this.topic = properties.get("topic");
        
        // Add debug logging
        logger.info("Created Kafka config with properties: {}", this.props);
    }

    @Override
    public void init() {
        if (producer == null) {
            producer = new KafkaProducer<>(props);
            logger.info("Initialized Kafka producer for topic: {}", topic);
        }
    }

    @Override
    public void receive(String item) {
        try {
            if (item == null || item.isEmpty()) {
                logger.warn("Received empty item, skipping");
                return;
            }

            producer.send(new ProducerRecord<>(topic, item, item));
            logger.debug("Sent message to Kafka topic: {}", topic);
        } catch (Exception e) {
            logger.error("Error sending to Kafka: {}", e.getMessage());
            throw new RuntimeException("Failed to send message to Kafka", e);
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
            producer = null;
            logger.info("Closed Kafka producer");
        }
    }
} 