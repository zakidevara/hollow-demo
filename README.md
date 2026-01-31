# Netflix Hollow Demo Application

A Spring Boot application demonstrating Netflix Hollow - an in-memory immutable dataset caching library.

## What is Netflix Hollow?

[Netflix Hollow](https://hollow.how) is a Java library and toolkit for disseminating in-memory datasets from a single producer to many consumers for high-performance read-only access. It provides:

- **Efficient Memory Usage**: Data is stored in a compact binary format
- **Delta Updates**: Only changes are transmitted when data is updated
- **Time Travel**: Access historical versions of data
- **Type Safety**: Strongly typed data access with generated APIs

## Project Structure

```
src/main/java/com/devara/hollow/
├── HollowApplication.java       # Spring Boot main application
├── HollowProducerService.java   # Produces/publishes data to Hollow
├── HollowConsumerService.java   # Consumes/reads data from Hollow
├── HollowController.java        # REST API endpoints
├── config/
│   └── HollowUIConfiguration.java  # Configuration class
└── model/
    └── UserAccount.java         # Sample data model
```

## How It Works

### 1. Producer

The `HollowProducerService` is responsible for publishing data to Hollow:

- Uses a filesystem-based publisher to store data snapshots in `./hollow-repo`
- Runs "cycles" where all current data is added to Hollow
- Each cycle creates a new version with an associated timestamp

### 2. Consumer

The `HollowConsumerService` reads data from Hollow:

- Uses a filesystem-based blob retriever to read data from `./hollow-repo`
- Watches for announcements of new data versions
- Can be refreshed to get the latest data

### 3. Data Flow

```
┌──────────────┐    publish    ┌──────────────────┐    read     ┌──────────────┐
│   Producer   │ ──────────► │  hollow-repo/    │ ──────────► │   Consumer   │
│   Service    │              │  (filesystem)    │             │   Service    │
└──────────────┘              └──────────────────┘             └──────────────┘
```

## Prerequisites

- Java 21 or higher
- Gradle (wrapper included)

## Running the Application

1. **Build the project**:
   ```bash
   ./gradlew build
   ```

2. **Run the application**:
   ```bash
   ./gradlew bootRun
   ```

   The application starts on port **8080**.

## REST API Endpoints

### Check Status
```http
GET http://localhost:8080/api/hollow/status
```

Returns the current state of the Hollow consumer:
```json
{
  "currentVersion": 20260131143138001,
  "hasData": true,
  "types": ["String", "UserAccount"]
}
```

### Publish Data
```http
POST http://localhost:8080/api/hollow/publish
Content-Type: application/json
```

**Without body** - publishes sample data:
```json
{
  "message": "Data published successfully",
  "recordCount": 5
}
```

**With custom data**:
```json
[
  {"id": 1, "username": "alice", "active": true},
  {"id": 2, "username": "bob", "active": false}
]
```

### Refresh Consumer
```http
POST http://localhost:8080/api/hollow/refresh
```

Refreshes the consumer to pick up the latest published data:
```json
{
  "message": "Consumer refreshed successfully",
  "currentVersion": 20260131143138001
}
```

### Browse Data
```http
GET http://localhost:8080/api/hollow/data
GET http://localhost:8080/api/hollow/data?type=UserAccount
```

Returns all data stored in Hollow:
```json
{
  "currentVersion": 20260131143138001,
  "hasData": true,
  "data": {
    "UserAccount": [
      {"ordinal": 0, "id": 1, "username": "alice", "active": true},
      {"ordinal": 1, "id": 2, "username": "bob", "active": true}
    ]
  }
}
```

## Quick Start Example

1. Start the application:
   ```bash
   ./gradlew bootRun
   ```

2. Check initial status (no data):
   ```bash
   curl http://localhost:8080/api/hollow/status
   ```

3. Publish sample data:
   ```bash
   curl -X POST http://localhost:8080/api/hollow/publish
   ```

4. Refresh the consumer:
   ```bash
   curl -X POST http://localhost:8080/api/hollow/refresh
   ```

5. Browse the data:
   ```bash
   curl http://localhost:8080/api/hollow/data
   ```

## Data Model

The `UserAccount` model demonstrates Hollow's features:

```java
@HollowPrimaryKey(fields="id")
public class UserAccount {
    private long id;
    private String username;
    private boolean active;
}
```

- `@HollowPrimaryKey` - Designates `id` as the primary key for deduplication

## Data Storage

Data is stored in the `./hollow-repo` directory:
- `snapshot-*` files contain full data snapshots
- `delta-*` files contain incremental changes
- `announced.version` tracks the latest published version

## Key Concepts

### Ordinals
Each record in Hollow has an ordinal (index) that identifies it within a type. Ordinals are stable across refreshes as long as the data doesn't change.

### Versions
Each publish cycle creates a new version. The version number is typically a timestamp. Consumers can refresh to get the latest version.

### Types
Hollow automatically creates types based on your data model classes. Primitive wrappers like `String` become their own types.

## Technologies Used

- **Spring Boot 4.0** - Application framework
- **Netflix Hollow 7.14.x** - In-memory dataset library
- **Lombok** - Reduces boilerplate code
- **Gradle** - Build tool

## License

This project is for demonstration purposes.

## Resources

- [Netflix Hollow Documentation](https://hollow.how)
- [Hollow GitHub Repository](https://github.com/Netflix/hollow)
- [Spring Boot Documentation](https://spring.io/projects/spring-boot)
