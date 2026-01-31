package com.devara.hollow.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka configuration for KTable-based auto-refresh.
 * Active only when 'kafka' profile is enabled.
 */
@Configuration
@Profile("kafka")
@EnableKafka
@EnableKafkaStreams
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${kafka.application-id:hollow-ktable-app}")
    private String applicationId;

    @Value("${kafka.topic.user-accounts:user-accounts}")
    private String userAccountsTopic;

    @Value("${kafka.state-dir:./kafka-streams-state}")
    private String stateDir;

    /**
     * Kafka Admin for topic management.
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    /**
     * Create the user-accounts topic as a compacted topic.
     * Compaction ensures latest value per key is retained.
     */
    @Bean
    public NewTopic userAccountsTopic() {
        return TopicBuilder.name(userAccountsTopic)
                .partitions(3)
                .replicas(1)
                .compact()
                .build();
    }

    /**
     * Kafka Streams configuration for KTable operations.
     */
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        
        // Processing guarantee - exactly once for stronger consistency
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        
        // Commit interval for state stores (lower = faster visibility, higher = better throughput)
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        
        // Number of standby replicas for fault tolerance
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);
        
        return new KafkaStreamsConfiguration(props);
    }

    public String getUserAccountsTopic() {
        return userAccountsTopic;
    }
}
