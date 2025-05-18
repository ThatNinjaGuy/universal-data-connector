package com.example.pipeline.factory.source;

import com.hazelcast.jet.pipeline.SourceBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class KafkaSourceContext implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSourceContext.class);
    private final Properties props;
    private final String topic;
    private KafkaConsumer<String, String> consumer;

    public KafkaSourceContext(Map<String, String> properties) {
        this.props = new Properties();
        
        this.props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get("bootstrapServers"));
        this.props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.get("groupId"));
        this.props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.get("autoOffsetReset"));
        this.props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        this.topic = properties.get("topic");
        
        logger.info("Created Kafka config with properties: {}", this.props);
    }

    public void init() {
        Properties consumerProps = new Properties();
        consumerProps.putAll(props);
        
        logger.info("Initializing Kafka consumer with properties: {}", consumerProps);
        
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(topic));
        
        logger.info("Successfully created Kafka consumer for topic: {}", topic);
    }

    public void fillBuffer(SourceBuilder.SourceBuffer<String> buffer) {
        try {
            for (ConsumerRecord<String, String> record : consumer.poll(java.time.Duration.ofMillis(100))) {
                buffer.add(record.value());
            }
        } catch (Exception e) {
            logger.warn("Error polling Kafka: {}", e.getMessage());
        }
    }

    public void close() {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
    }
} 