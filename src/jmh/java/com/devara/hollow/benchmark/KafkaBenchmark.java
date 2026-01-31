package com.devara.hollow.benchmark;

import com.devara.hollow.model.UserAccount;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * JMH Benchmark for Kafka KTable operations.
 * Measures write latency, read latency, and auto-refresh performance.
 * 
 * NOTE: Requires a running Kafka broker at localhost:9092
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class KafkaBenchmark {

    private static final int RECORD_COUNT = 10000;
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "benchmark-user-accounts";
    private static final String STORE_NAME = "benchmark-store";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private Path stateDir;
    private KafkaProducer<Long, String> producer;
    private KafkaStreams streams;
    private List<UserAccount> testData;
    private volatile boolean streamsReady = false;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        // Create temp directory for state store
        stateDir = Files.createTempDirectory("kafka-benchmark");
        
        // Generate test data
        testData = new ArrayList<>(RECORD_COUNT);
        for (int i = 0; i < RECORD_COUNT; i++) {
            testData.add(new UserAccount(i, "user_" + i, i % 2 == 0));
        }
        
        // Create topic if not exists
        createTopicIfNotExists();
        
        // Initialize producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producer = new KafkaProducer<>(producerProps);
        
        // Publish initial data
        for (UserAccount account : testData) {
            String json = objectMapper.writeValueAsString(account);
            producer.send(new ProducerRecord<>(TOPIC, account.getId(), json));
        }
        producer.flush();
        
        // Initialize Kafka Streams with KTable
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "benchmark-app-" + System.currentTimeMillis());
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        streamsProps.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        streamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        StreamsBuilder builder = new StreamsBuilder();
        builder.table(
                TOPIC,
                Consumed.with(Serdes.Long(), Serdes.String()),
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
                        .withKeySerde(Serdes.Long())
                        .withValueSerde(Serdes.String())
        );
        
        streams = new KafkaStreams(builder.build(), streamsProps);
        
        // Wait for streams to be ready
        CountDownLatch latch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                streamsReady = true;
                latch.countDown();
            }
        });
        
        streams.start();
        latch.await(30, TimeUnit.SECONDS);
        
        // Wait for data to be available
        Thread.sleep(2000);
    }

    private void createTopicIfNotExists() {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        
        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existingTopics = admin.listTopics().names().get();
            if (!existingTopics.contains(TOPIC)) {
                NewTopic newTopic = new NewTopic(TOPIC, 3, (short) 1);
                newTopic.configs(Map.of("cleanup.policy", "compact"));
                admin.createTopics(List.of(newTopic)).all().get();
                Thread.sleep(1000); // Wait for topic to be created
            }
        } catch (Exception e) {
            System.err.println("Warning: Could not create topic: " + e.getMessage());
        }
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException {
        if (producer != null) {
            producer.close();
        }
        if (streams != null) {
            streams.close();
        }
        
        // Cleanup state directory
        if (stateDir != null) {
            Files.walk(stateDir)
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore cleanup errors
                        }
                    });
        }
    }

    private ReadOnlyKeyValueStore<Long, String> getStore() {
        return streams.store(
                StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.keyValueStore())
        );
    }

    /**
     * Benchmark: Write latency for publishing a batch of records.
     */
    @Benchmark
    public void writeLatency(Blackhole blackhole) throws Exception {
        for (UserAccount account : testData) {
            String json = objectMapper.writeValueAsString(account);
            producer.send(new ProducerRecord<>(TOPIC, account.getId(), json));
        }
        producer.flush();
        blackhole.consume(testData.size());
    }

    /**
     * Benchmark: Single read latency from KTable store.
     */
    @Benchmark
    public void singleReadLatency(Blackhole blackhole) {
        long key = System.nanoTime() % RECORD_COUNT;
        String value = getStore().get(key);
        blackhole.consume(value);
    }

    /**
     * Benchmark: Bulk read - iterate all records.
     */
    @Benchmark
    public void bulkReadLatency(Blackhole blackhole) {
        var store = getStore();
        int count = 0;
        try (var iterator = store.all()) {
            while (iterator.hasNext()) {
                var entry = iterator.next();
                count++;
            }
        }
        blackhole.consume(count);
    }
}
