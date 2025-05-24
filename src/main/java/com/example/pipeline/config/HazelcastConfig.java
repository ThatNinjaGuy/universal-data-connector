package com.example.pipeline.config;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Hazelcast;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HazelcastConfig {

    @Bean
    public HazelcastInstance hazelcastInstance() {
        // Create base Hazelcast config
        Config config = new Config();
        config.setClusterName("pipeline-cluster");
        
        // Configure networking
        config.getNetworkConfig()
            .setPort(5701)
            .getJoin()
            .getMulticastConfig()
            .setEnabled(false);
        
        config.getNetworkConfig()
            .getJoin()
            .getTcpIpConfig()
            .setEnabled(true)
            .addMember("127.0.0.1");

        // Enable Jet
        config.getJetConfig()
            .setEnabled(true)
            .setResourceUploadEnabled(true);

        // Create Hazelcast instance
        return Hazelcast.newHazelcastInstance(config);
    }
} 