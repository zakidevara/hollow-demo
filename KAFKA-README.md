# Kafka KTable Auto-Refresh Demo

This document describes how to run the Kafka KTable implementation as an alternative to the filesystem-based Hollow approach.

## Key Difference: Auto-Refresh

| Aspect | Hollow (Filesystem) | Kafka KTable |
|--------|---------------------|--------------|
| Refresh mechanism | File watcher + `triggerRefresh()` | Automatic streaming updates |
| Latency | 1-3 seconds | Milliseconds |
| Manual refresh needed | No (with watcher) | Never |
| State storage | Binary blobs | RocksDB (local) |

## Prerequisites

### 1. Start Kafka

Start Kafka using Docker Compose:

```powershell
docker-compose -f docker-compose-kafka.yml up -d
```

This starts:
- Kafka broker (KRaft mode, no Zookeeper) on port 9092
- Kafka UI on port 8090 (optional, for monitoring)

### 2. Start the Application

```powershell
./gradlew bootRun --args='--spring.profiles.active=kafka'
```

Wait for the application to start and the KTable to reach RUNNING state.

## Running the Demo

```powershell
./demo-kafka-auto-refresh.ps1
```

## REST API Endpoints

### Publish Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/kafka/publish` | Publish multiple users |
| POST | `/api/kafka/users` | Create/update single user |
| DELETE | `/api/kafka/users/{id}` | Delete user (tombstone) |

### Query Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/kafka/users` | Get all users |
| GET | `/api/kafka/users/{id}` | Get user by ID |
| GET | `/api/kafka/users/range?from=X&to=Y` | Get users in ID range |
| GET | `/api/kafka/users/active` | Get active users only |
| GET | `/api/kafka/status` | Check KTable status |

## Example Usage

### Publish a user

```powershell
Invoke-RestMethod -Method POST -Uri "http://localhost:8080/api/kafka/users" `
  -ContentType "application/json" `
  -Body '{"id": 1, "username": "alice", "active": true}'
```

### Query immediately (no refresh needed!)

```powershell
Invoke-RestMethod -Uri "http://localhost:8080/api/kafka/users/1"
```

### Update the user

```powershell
Invoke-RestMethod -Method POST -Uri "http://localhost:8080/api/kafka/users" `
  -ContentType "application/json" `
  -Body '{"id": 1, "username": "alice_updated", "active": false}'
```

### Query again (auto-refreshed!)

```powershell
Invoke-RestMethod -Uri "http://localhost:8080/api/kafka/users/1"
# Returns updated data - no refresh call needed!
```

## Architecture

```
┌─────────────────┐         ┌──────────────────────────────┐
│ REST API        │         │ Kafka                        │
│ (Producer)      │────────▶│ Topic: user-accounts         │
│                 │  write  │ (compacted, key=userId)      │
└─────────────────┘         └──────────────────────────────┘
                                         │
                                         ▼
                            ┌─────────────────────────────┐
                            │ Kafka Streams               │
                            │ KTable (auto-materialized)  │
                            │ ┌─────────────────────────┐ │
                            │ │ RocksDB State Store     │ │
                            │ │ (always up-to-date)     │ │
                            │ └─────────────────────────┘ │
                            └─────────────────────────────┘
                                         │
                                         ▼
                            ┌─────────────────────────────┐
                            │ REST API                    │
                            │ (Consumer queries)          │
                            └─────────────────────────────┘
```

## How KTable Auto-Refresh Works

1. **Producer** publishes records to the `user-accounts` topic (keyed by user ID)
2. **Kafka Streams** consumes the topic and materializes it as a KTable
3. **State Store** (RocksDB) is automatically updated for each new record
4. **Queries** read from the state store - always returning the latest data

No polling, no file watching, no manual refresh!

## Monitoring

### Kafka UI

Access Kafka UI at http://localhost:8090 to:
- View topics and partitions
- Browse messages
- Monitor consumer lag

### Application Logs

Enable debug logging to see KTable updates:

```properties
logging.level.com.devara.hollow.service.kafka=DEBUG
```

## Cleanup

Stop Kafka:

```powershell
docker-compose -f docker-compose-kafka.yml down -v
```

## Running Benchmarks

Compare Hollow vs Kafka performance:

```powershell
./gradlew jmh
```

Results will be in `build/results/jmh/results.json`.
