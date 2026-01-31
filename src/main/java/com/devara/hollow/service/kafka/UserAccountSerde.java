package com.devara.hollow.service.kafka;

import com.devara.hollow.model.UserAccount;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Custom Serde (Serializer/Deserializer) for UserAccount objects.
 * Uses JSON format for human-readable Kafka messages.
 */
public class UserAccountSerde implements Serde<UserAccount> {

    private static final Logger logger = LoggerFactory.getLogger(UserAccountSerde.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Serializer<UserAccount> serializer() {
        return new UserAccountSerializer();
    }

    @Override
    public Deserializer<UserAccount> deserializer() {
        return new UserAccountDeserializer();
    }

    /**
     * Serializer that converts UserAccount to JSON bytes.
     */
    public static class UserAccountSerializer implements Serializer<UserAccount> {

        @Override
        public byte[] serialize(String topic, UserAccount data) {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (IOException e) {
                logger.error("Error serializing UserAccount: {}", e.getMessage(), e);
                throw new RuntimeException("Failed to serialize UserAccount", e);
            }
        }
    }

    /**
     * Deserializer that converts JSON bytes back to UserAccount.
     */
    public static class UserAccountDeserializer implements Deserializer<UserAccount> {

        @Override
        public UserAccount deserialize(String topic, byte[] data) {
            if (data == null || data.length == 0) {
                return null;
            }
            try {
                return objectMapper.readValue(data, UserAccount.class);
            } catch (IOException e) {
                logger.error("Error deserializing UserAccount from topic {}: {}", topic, e.getMessage(), e);
                // Log the raw data for debugging
                logger.debug("Raw data: {}", new String(data, StandardCharsets.UTF_8));
                throw new RuntimeException("Failed to deserialize UserAccount", e);
            }
        }
    }
}
