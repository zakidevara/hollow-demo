package com.devara.hollow.service.kafka;

import com.devara.hollow.model.UserAccount;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Kafka Streams-based consumer service that uses KTable for auto-refreshing state.
 * 
 * KTables automatically update when new records arrive in the topic - no manual
 * refresh needed! This provides real-time visibility of data changes.
 */
@Service
@Profile("kafka")
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    private static final String STORE_NAME = "user-accounts-store";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${kafka.topic.user-accounts:user-accounts}")
    private String userAccountsTopic;

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    private StreamsBuilder streamsBuilder;

    private volatile boolean initialized = false;

    @PostConstruct
    public void init() {
        logger.info("Initializing Kafka Streams KTable for topic: {}", userAccountsTopic);
        
        // Define the KTable - it automatically updates as new records arrive!
        KTable<Long, String> userTable = streamsBuilder.table(
                userAccountsTopic,
                Consumed.with(Serdes.Long(), Serdes.String()),
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
                        .withKeySerde(Serdes.Long())
                        .withValueSerde(Serdes.String())
        );

        // Log state changes for monitoring
        userTable.toStream().foreach((key, value) -> {
            if (value == null) {
                logger.debug("UserAccount {} deleted from KTable", key);
            } else {
                logger.debug("UserAccount {} updated in KTable", key);
            }
        });

        // Add state listener
        streamsBuilderFactoryBean.setStateListener((newState, oldState) -> {
            logger.info("Kafka Streams state changed: {} -> {}", oldState, newState);
            if (newState == KafkaStreams.State.RUNNING) {
                initialized = true;
                logger.info("KTable consumer is now RUNNING and ready for queries");
            }
        });

        logger.info("KTable defined for store: {}", STORE_NAME);
    }

    /**
     * Check if the KTable store is ready for queries.
     */
    public boolean isReady() {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        return streams != null && streams.state() == KafkaStreams.State.RUNNING;
    }

    /**
     * Get the state store for querying.
     * 
     * @return the read-only key-value store
     * @throws IllegalStateException if the store is not ready
     */
    private ReadOnlyKeyValueStore<Long, String> getStore() {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
            throw new IllegalStateException("KTable store is not ready. Current state: " + 
                    (streams != null ? streams.state() : "null"));
        }
        return streams.store(
                StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.keyValueStore())
        );
    }

    /**
     * Get a UserAccount by ID.
     * Returns the latest value - automatically refreshed when new records arrive.
     * 
     * @param userId the user ID
     * @return Optional containing the UserAccount if found
     */
    public Optional<UserAccount> findById(long userId) {
        try {
            String json = getStore().get(userId);
            if (json == null) {
                return Optional.empty();
            }
            return Optional.of(objectMapper.readValue(json, UserAccount.class));
        } catch (Exception e) {
            logger.error("Error reading UserAccount {}: {}", userId, e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Get all UserAccounts from the KTable.
     * 
     * @return list of all UserAccounts
     */
    public List<UserAccount> findAll() {
        List<UserAccount> results = new ArrayList<>();
        try (KeyValueIterator<Long, String> iterator = getStore().all()) {
            while (iterator.hasNext()) {
                var entry = iterator.next();
                try {
                    UserAccount account = objectMapper.readValue(entry.value, UserAccount.class);
                    results.add(account);
                } catch (Exception e) {
                    logger.error("Error deserializing UserAccount {}: {}", entry.key, e.getMessage());
                }
            }
        }
        return results;
    }

    /**
     * Get the approximate number of entries in the store.
     * 
     * @return the count of entries
     */
    public long count() {
        return getStore().approximateNumEntries();
    }

    /**
     * Find UserAccounts within a range of IDs.
     * 
     * @param fromId starting ID (inclusive)
     * @param toId ending ID (inclusive)
     * @return list of UserAccounts in the range
     */
    public List<UserAccount> findByIdRange(long fromId, long toId) {
        List<UserAccount> results = new ArrayList<>();
        try (KeyValueIterator<Long, String> iterator = getStore().range(fromId, toId)) {
            while (iterator.hasNext()) {
                var entry = iterator.next();
                try {
                    UserAccount account = objectMapper.readValue(entry.value, UserAccount.class);
                    results.add(account);
                } catch (Exception e) {
                    logger.error("Error deserializing UserAccount {}: {}", entry.key, e.getMessage());
                }
            }
        }
        return results;
    }

    /**
     * Find active UserAccounts.
     * Note: This scans all entries since KTable doesn't support secondary indexes.
     * 
     * @return list of active UserAccounts
     */
    public List<UserAccount> findActive() {
        return findAll().stream()
                .filter(UserAccount::isActive)
                .toList();
    }

    /**
     * Get the current Kafka Streams state.
     */
    public String getState() {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        return streams != null ? streams.state().toString() : "NOT_STARTED";
    }
}
