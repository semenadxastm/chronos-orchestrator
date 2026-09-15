# Chronos Orchestrator - AI Developer Context

## 1. Project Identity & Architecture
Chronos is a distributed, event-sourced workflow orchestrator modeled after Temporal/Uber Cadence. It allows developers to write standard Java code that is immune to infrastructure failures. 

**Core Mechanism (Durable Execution):**
1. Workflows are state machines backed by an append-only event log (PostgreSQL).
2. The Server manages state and queues tasks.
3. External Workers poll the Server via gRPC to receive tasks.
4. If a worker crashes, the Server detects the timeout, re-queues the task, and a new worker replays the event history to resume execution.

## 2. Tech Stack & Infrastructure
* **Language:** Java 21 (Strictly utilize Virtual Threads for blocking I/O and polling)(Change java versions only using sdkman thats already installed).
* **Build Tool:** Gradle 8.6 (Multi-module).
* **Framework:** Spring Boot 3.2.x.
* **Database:** PostgreSQL (for the Event Store / Append-only log).
* **Message Broker:** Kafka (for high-throughput task queuing between server nodes).
* **RPC:** gRPC / Protobuf (Communication between Server and Worker Client).

## 3. Module Structure
* `chronos-proto`: Defines `.proto` schemas and generates gRPC Java stubs.
* `chronos-server`: The central coordinator. Exposes gRPC endpoints, manages Postgres state, and interfaces with Kafka.
* `chronos-worker-client`: A lightweight SDK. Contains a polling loop (using Virtual Threads) to fetch tasks from the server and report results.
* `demo-app`: A sample application (e.g., e-commerce order fulfillment) that imports the worker-client to test the engine.

## 4. Testing Strategy
* **Unit Tests:** JUnit 5 and Mockito for state machine logic and pure Java services.
* **Integration Tests:** Testcontainers for spinning up ephemeral PostgreSQL and Kafka instances. 
* **gRPC Testing:** `grpc-testing` utilities to mock worker-to-server communication.

## 5. Coding Conventions
* Use Lombok (`@Data`, `@Builder`, `@Slf4j`) to reduce boilerplate.
* Prefer Java 21 `switch` expressions and pattern matching where applicable.
* Never block OS threads for I/O; ensure virtual threads are enabled (`spring.threads.virtual.enabled=true`).
* All dates/times must be stored in UTC (`Instant`).