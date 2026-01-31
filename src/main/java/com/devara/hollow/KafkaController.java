package com.devara.hollow;

import com.devara.hollow.model.UserAccount;
import com.devara.hollow.service.kafka.KafkaConsumerService;
import com.devara.hollow.service.kafka.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * REST Controller for Kafka KTable operations.
 * Provides endpoints for publishing and querying UserAccounts via Kafka.
 * Active only when 'kafka' profile is enabled.
 */
@RestController
@RequestMapping("/api/kafka")
@Profile("kafka")
public class KafkaController {

    private final KafkaProducerService producerService;
    private final KafkaConsumerService consumerService;

    @Autowired
    public KafkaController(KafkaProducerService producerService, KafkaConsumerService consumerService) {
        this.producerService = producerService;
        this.consumerService = consumerService;
    }

    /**
     * Publish user accounts to Kafka topic.
     * Records are immediately available to KTable consumers - no refresh needed!
     */
    @PostMapping("/publish")
    public ResponseEntity<Map<String, Object>> publishData(@RequestBody(required = false) List<UserAccount> users) {
        if (users == null || users.isEmpty()) {
            users = generateSampleData();
        }

        producerService.runCycle(users);

        Map<String, Object> response = new HashMap<>();
        response.put("message", "Data published to Kafka");
        response.put("recordCount", users.size());
        response.put("note", "KTable consumers will auto-refresh - no manual refresh needed!");
        return ResponseEntity.ok(response);
    }

    /**
     * Publish a single user account.
     */
    @PostMapping("/users")
    public ResponseEntity<Map<String, Object>> createUser(@RequestBody UserAccount user) {
        producerService.publish(user);

        Map<String, Object> response = new HashMap<>();
        response.put("message", "User published to Kafka");
        response.put("user", user);
        return ResponseEntity.ok(response);
    }

    /**
     * Delete a user (publishes tombstone).
     */
    @DeleteMapping("/users/{userId}")
    public ResponseEntity<Map<String, Object>> deleteUser(@PathVariable long userId) {
        producerService.delete(userId);

        Map<String, Object> response = new HashMap<>();
        response.put("message", "Tombstone published for user");
        response.put("userId", userId);
        return ResponseEntity.ok(response);
    }

    /**
     * Get all users from KTable.
     * Always returns the latest data - auto-refreshed!
     */
    @GetMapping("/users")
    public ResponseEntity<List<UserAccount>> getAllUsers() {
        return ResponseEntity.ok(consumerService.findAll());
    }

    /**
     * Get a user by ID.
     */
    @GetMapping("/users/{userId}")
    public ResponseEntity<UserAccount> getUserById(@PathVariable long userId) {
        return consumerService.findById(userId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Get users within an ID range.
     */
    @GetMapping("/users/range")
    public ResponseEntity<List<UserAccount>> getUsersInRange(
            @RequestParam long from,
            @RequestParam long to) {
        return ResponseEntity.ok(consumerService.findByIdRange(from, to));
    }

    /**
     * Get all active users.
     */
    @GetMapping("/users/active")
    public ResponseEntity<List<UserAccount>> getActiveUsers() {
        return ResponseEntity.ok(consumerService.findActive());
    }

    /**
     * Get the current KTable status.
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> response = new HashMap<>();
        response.put("state", consumerService.getState());
        response.put("ready", consumerService.isReady());
        
        if (consumerService.isReady()) {
            response.put("approximateCount", consumerService.count());
        }
        
        response.put("note", "KTable auto-refreshes - always shows latest data!");
        return ResponseEntity.ok(response);
    }

    /**
     * Generate sample user data.
     */
    private List<UserAccount> generateSampleData() {
        List<UserAccount> users = new ArrayList<>();
        users.add(new UserAccount(1, "alice", true));
        users.add(new UserAccount(2, "bob", true));
        users.add(new UserAccount(3, "charlie", false));
        users.add(new UserAccount(4, "diana", true));
        users.add(new UserAccount(5, "eve", false));
        return users;
    }
}
