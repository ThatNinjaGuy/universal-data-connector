package com.example.pipeline.config;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HazelcastConfig {

    @Bean
    public JetInstance jetInstance() {
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

        // Create Jet instance using the static factory method
        return Jet.bootstrappedInstance();
    }

    @Bean
    public HazelcastInstance hazelcastInstance(JetInstance jetInstance) {
        return jetInstance.getHazelcastInstance();
    }
} 