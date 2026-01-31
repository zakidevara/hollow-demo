package com.devara.hollow.service.kafka;

import com.devara.hollow.model.UserAccount;
import com.devara.hollow.service.HollowProducerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Kafka-based producer service that publishes UserAccount records to a compacted topic.
 * Each UserAccount is keyed by its ID, enabling KTable materialization on consumers.
 */
@Service
@Profile("kafka")
public class KafkaProducerService implements HollowProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${kafka.topic.user-accounts:user-accounts}")
    private String userAccountsTopic;

    private KafkaProducer<Long, String> producer;

    @Override
    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Enable idempotence for exactly-once semantics
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        
        // Batch settings for throughput
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        
        producer = new KafkaProducer<>(props);
        logger.info("Kafka producer initialized for topic: {}", userAccountsTopic);
    }

    @Override
    public void runCycle(List<UserAccount> data) {
        logger.info("Publishing {} UserAccount records to Kafka topic: {}", data.size(), userAccountsTopic);
        
        long startTime = System.currentTimeMillis();
        int successCount = 0;
        int failureCount = 0;

        for (UserAccount account : data) {
            try {
                String json = objectMapper.writeValueAsString(account);
                ProducerRecord<Long, String> record = new ProducerRecord<>(
                        userAccountsTopic,
                        account.getId(),
                        json
                );

                Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to publish UserAccount {}: {}", 
                                account.getId(), exception.getMessage());
                    } else {
                        logger.debug("Published UserAccount {} to partition {} offset {}", 
                                account.getId(), metadata.partition(), metadata.offset());
                    }
                });

                successCount++;
            } catch (Exception e) {
                logger.error("Error publishing UserAccount {}: {}", account.getId(), e.getMessage(), e);
                failureCount++;
            }
        }

        // Flush to ensure all records are sent
        producer.flush();
        
        long duration = System.currentTimeMillis() - startTime;
        logger.info("Kafka publish complete: {} succeeded, {} failed in {}ms", 
                successCount, failureCount, duration);
    }

    /**
     * Publish a single UserAccount record.
     * 
     * @param account the UserAccount to publish
     */
    public void publish(UserAccount account) {
        runCycle(List.of(account));
    }

    /**
     * Delete a UserAccount by publishing a tombstone (null value).
     * This triggers removal from KTable consumers.
     * 
     * @param userId the ID of the user to delete
     */
    public void delete(long userId) {
        try {
            ProducerRecord<Long, String> tombstone = new ProducerRecord<>(
                    userAccountsTopic,
                    userId,
                    null  // null value = tombstone
            );
            producer.send(tombstone);
            producer.flush();
            logger.info("Published tombstone for UserAccount {}", userId);
        } catch (Exception e) {
            logger.error("Error publishing tombstone for UserAccount {}: {}", userId, e.getMessage(), e);
        }
    }

    /**
     * Close the producer and release resources.
     */
    public void close() {
        if (producer != null) {
            producer.close();
            logger.info("Kafka producer closed");
        }
    }
}
