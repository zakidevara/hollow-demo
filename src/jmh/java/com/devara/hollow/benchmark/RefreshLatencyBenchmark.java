package com.devara.hollow.benchmark;

import com.devara.hollow.model.UserAccount;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemAnnouncementWatcher;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemBlobRetriever;
import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemAnnouncer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemPublisher;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * JMH Benchmark comparing refresh latency between Hollow and Kafka KTable.
 * Measures end-to-end time from publish to consumer visibility.
 * 
 * This is the key metric for comparing the two approaches:
 * - Hollow: publish -> file write -> announcement -> poll/watch -> refresh
 * - Kafka: publish -> Kafka broker -> consumer stream -> KTable update
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class RefreshLatencyBenchmark {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "refresh-benchmark-users";
    private static final String STORE_NAME = "refresh-benchmark-store";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private Path hollowDir;
    private Path kafkaStateDir;
    
    // Hollow components
    private HollowProducer hollowProducer;
    private HollowConsumer hollowConsumer;
    
    // Kafka components
    private KafkaProducer<Long, String> kafkaProducer;
    private KafkaStreams kafkaStreams;
    
    private AtomicLong recordIdCounter = new AtomicLong(0);

    @Setup(Level.Trial)
    public void setup() throws Exception {
        hollowDir = Files.createTempDirectory("hollow-refresh-benchmark");
        kafkaStateDir = Files.createTempDirectory("kafka-refresh-benchmark");
        
        setupHollow();
        setupKafka();
    }

    private void setupHollow() {
        HollowFilesystemPublisher publisher = new HollowFilesystemPublisher(hollowDir);
        HollowFilesystemAnnouncer announcer = new HollowFilesystemAnnouncer(hollowDir);
        
        hollowProducer = HollowProducer.withPublisher(publisher)
                .withAnnouncer(announcer)
                .build();
        hollowProducer.initializeDataModel(UserAccount.class);
        
        // Initial cycle
        hollowProducer.runCycle(state -> {
            state.add(new UserAccount(0, "initial", true));
        });
        
        HollowFilesystemBlobRetriever blobRetriever = new HollowFilesystemBlobRetriever(hollowDir);
        HollowFilesystemAnnouncementWatcher announcementWatcher = 
                new HollowFilesystemAnnouncementWatcher(hollowDir);
        
        hollowConsumer = HollowConsumer.withBlobRetriever(blobRetriever)
                .withAnnouncementWatcher(announcementWatcher)
                .build();
        hollowConsumer.triggerRefresh();
    }

    private void setupKafka() throws Exception {
        // Create topic
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        
        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existingTopics = admin.listTopics().names().get();
            if (!existingTopics.contains(TOPIC)) {
                NewTopic newTopic = new NewTopic(TOPIC, 1, (short) 1);
                newTopic.configs(Map.of("cleanup.policy", "compact"));
                admin.createTopics(List.of(newTopic)).all().get();
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            System.err.println("Warning: Could not create topic: " + e.getMessage());
        }
        
        // Producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        kafkaProducer = new KafkaProducer<>(producerProps);
        
        // Streams
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "refresh-benchmark-" + System.currentTimeMillis());
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        streamsProps.put(StreamsConfig.STATE_DIR_CONFIG, kafkaStateDir.toString());
        streamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 50);
        streamsProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        StreamsBuilder builder = new StreamsBuilder();
        builder.table(
                TOPIC,
                Consumed.with(Serdes.Long(), Serdes.String()),
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
                        .withKeySerde(Serdes.Long())
                        .withValueSerde(Serdes.String())
        );
        
        kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        
        CountDownLatch latch = new CountDownLatch(1);
        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                latch.countDown();
            }
        });
        
        kafkaStreams.start();
        latch.await(30, TimeUnit.SECONDS);
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException {
        if (kafkaProducer != null) kafkaProducer.close();
        if (kafkaStreams != null) kafkaStreams.close();
        
        cleanupDirectory(hollowDir);
        cleanupDirectory(kafkaStateDir);
    }

    private void cleanupDirectory(Path dir) throws IOException {
        if (dir != null && Files.exists(dir)) {
            Files.walk(dir)
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
        }
    }

    /**
     * Benchmark: Hollow end-to-end refresh latency.
     * Measures time from publish to consumer refresh.
     */
    @Benchmark
    public void hollowRefreshLatency(Blackhole blackhole) {
        long id = recordIdCounter.incrementAndGet();
        UserAccount account = new UserAccount(id, "user_" + id, true);
        
        // Publish
        hollowProducer.runCycle(state -> {
            state.add(account);
        });
        
        // Trigger refresh (simulates what the announcement watcher would do)
        hollowConsumer.triggerRefresh();
        
        blackhole.consume(hollowConsumer.getCurrentVersionId());
    }

    /**
     * Benchmark: Kafka KTable end-to-end refresh latency.
     * Measures time from publish to visibility in KTable store.
     */
    @Benchmark
    public void kafkaRefreshLatency(Blackhole blackhole) throws Exception {
        long id = recordIdCounter.incrementAndGet();
        UserAccount account = new UserAccount(id, "user_" + id, true);
        
        // Publish
        String json = objectMapper.writeValueAsString(account);
        kafkaProducer.send(new ProducerRecord<>(TOPIC, id, json)).get();
        
        // Poll until visible (with timeout)
        ReadOnlyKeyValueStore<Long, String> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.keyValueStore())
        );
        
        long startWait = System.currentTimeMillis();
        while (store.get(id) == null && (System.currentTimeMillis() - startWait) < 5000) {
            Thread.sleep(1);
        }
        
        blackhole.consume(store.get(id));
    }
}
