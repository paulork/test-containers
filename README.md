# tc-kafka: Testcontainers Demonstration Project

This project demonstrates the capabilities of Testcontainers for creating reliable integration tests in a Java Spring Boot application. It showcases how to integrate with various services like Kafka, PostgreSQL, AWS SNS/SQS (via LocalStack), and Redis, ensuring that tests run against real service instances managed by Docker.

The original goal was to demonstrate Testcontainers for Kafka, but it has been expanded to include examples for other common backing services.

## Key Technologies

*   **Java 11**
*   **Spring Boot 2.6.0**
*   **Maven** (Build and Dependency Management)
*   **Testcontainers** (Integration Testing Framework)
*   **JUnit 5** (Testing Framework)

## Services Tested

The project includes integration tests for:

*   **Apache Kafka:** For message queueing.
*   **PostgreSQL:** As a relational database.
*   **AWS SNS/SQS:** For cloud messaging, simulated using **LocalStack**.
*   **Redis:** As an in-memory data store.

## Testcontainers Usage

This project extensively uses [Testcontainers](https://www.testcontainers.org/) to facilitate reliable integration testing with real services running in Docker containers.

### Core Setup: `AbstractBaseTest.java`

The `AbstractBaseTest.java` class serves as the foundation for many of the integration tests.

*   **Shared Containers:** It defines static `@Container` instances for PostgreSQL (`postgres:14`) and Kafka (`confluentinc/cp-kafka:6.2.0`).
    *   The `withReuse(true)` flag is enabled for these containers. This means Testcontainers will try to reuse the same container across multiple test runs if possible, significantly speeding up the test suite execution after the first run.
*   **Dynamic Property Configuration:** It employs `@DynamicPropertySource` to dynamically update the Spring application context with the actual connection details (ports, hostnames, bootstrap servers) of the running TestContainers. This ensures that the application under test connects to the correct, dynamically allocated services.
    ```java
    @DynamicPropertySource
    static void datasourceConfig(DynamicPropertyRegistry registry) {
        Startables.deepStart(kafka, postgres); // Ensures containers are started

        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.kafka.producer.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.consumer.bootstrap-servers", kafka::getBootstrapServers);
    }
    ```
*   **Context Management:** The `@DirtiesContext` annotation is used, which ensures that the Spring application context is reloaded before each test class that inherits from `AbstractBaseTest`. This helps in maintaining test isolation.

### Service-Specific Test Setups

#### 1. Kafka (`KafkaTestContainersTest.java`)

*   Inherits from `AbstractBaseTest` to use the pre-configured Kafka container.
*   Tests involve sending messages to a Kafka topic using `KafkaTemplate` and verifying their reception by a `KafkaConsumer`.
*   [Awaitility](https://github.com/awaitility/awaitility) is used to handle the asynchronous nature of message consumption.

#### 2. PostgreSQL (`PostgresTestContainersTest.java`)

*   Inherits from `AbstractBaseTest` to use the pre-configured PostgreSQL container.
*   Tests focus on the data access layer, interacting with a `ProductRepository` (Spring Data JPA) to save and retrieve entities.
*   A simple query (`SELECT 1`) is also executed to confirm database connectivity.

#### 3. AWS SNS/SQS with LocalStack (`LocalstackSNSTestContainerTest.java`)

*   While it extends `AbstractBaseTest`, this class defines its own `LocalStackContainer` specifically for testing AWS SNS and SQS interactions.
    ```java
    @Container
    public static LocalStackContainer localStack = new LocalStackContainer(DockerImageName
            .parse("localstack/localstack:0.11.3"))
            .withServices(LocalStackContainer.Service.SNS, LocalStackContainer.Service.SQS)
            // ... other configurations
            .withReuse(true);
    ```
*   It configures `AmazonSNS` and `AmazonSQS` clients to point to the LocalStack container's endpoints.
*   In its `@BeforeAll` method, it programmatically creates SNS topics, SQS queues, and subscriptions for testing purposes.
*   Tests verify the successful creation and existence of these AWS resources within the LocalStack environment.
*   *Note: The Kafka and PostgreSQL containers from `AbstractBaseTest` are also started when this test class runs due to inheritance, but they are not directly used by these LocalStack-specific tests.*

#### 4. Redis (`RedisTestContainersTest.java`)

*   Similar to the LocalStack setup, this class extends `AbstractBaseTest` but defines its own `GenericContainer` for Redis (`redis:6.2.6`).
    ```java
    @Container
    public static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:6.2.6"))
            .withExposedPorts(6379)
            .withReuse(true);
    ```
*   A `Jedis` client is initialized to interact with the Redis container.
*   Tests cover basic Redis operations like setting, getting, updating, and deleting keys.
*   *Note: The Kafka and PostgreSQL containers from `AbstractBaseTest` are also started when this test class runs, but they are not directly used by these Redis-specific tests.*

## Configuration Files

The project uses several configuration files:

### 1. Spring Boot Application Properties

*   **`src/main/resources/application.yml`**:
    *   This is the primary configuration file for the Spring Boot application when it runs normally (outside of tests).
    *   It defines default connection details for services like PostgreSQL, Kafka, and AWS (SNS). For example, it might specify `localhost` for database connections or a default SNS topic ARN.
    *   These values are typically intended for a local development setup where services are run directly or via the `docker-compose.yml` described below.

*   **`src/test/resources/application.yml`**:
    *   This file provides configuration overrides specifically for the test environment.
    *   **Important Note:** In this project, this file is currently identical to the `main/resources/application.yml`. While Spring Boot tests will load properties from here, the TestContainers setup (especially in `AbstractBaseTest.java` and other test classes) uses `@DynamicPropertySource`. This dynamic mechanism overrides any conflicting properties (like database URLs or Kafka bootstrap servers) with the actual details from the TestContainers instances at runtime. This ensures tests connect to the containers, not to fixed `localhost` addresses that might not be correct or available.

### 2. Docker Compose for Local Development

*   **`docker-compose.yml` (in project root)**:
    *   This file, now located in the project's root directory, defines a multi-container Docker environment for local development. It includes services for:
        *   `postgres` (PostgreSQL database)
        *   `zookeeper`
        *   `kafka`
    *   This setup is convenient for running all necessary backing services locally without relying on TestContainers for everyday development or manual testing of the application.
    *   **Crucially, this `docker-compose.yml` is NOT directly used by the TestContainers setup in the Java test files.** The tests programmatically define and manage their own container instances using the TestContainers library.

## How to Run the Project

### Prerequisites

Before running the application or its tests, ensure you have the following installed:

*   **Java Development Kit (JDK):** Version 11 or higher (as specified in `pom.xml`).
*   **Maven:** To build the project, manage dependencies, and run tests.
*   **Docker:** Testcontainers relies on Docker to spin up instances of the services. Ensure Docker is installed and the Docker daemon is running.

### Running Integration Tests

The project includes a suite of integration tests that leverage Testcontainers.

*   **Using Maven:**
    Open your terminal, navigate to the project's root directory, and run:
    ```bash
    mvn test
    ```
    This command will compile the project, download dependencies, and execute all tests, including those using Testcontainers. Docker must be running for these tests to pass.

*   **Using an IDE (e.g., IntelliJ IDEA, Eclipse):**
    You can also run the tests directly from your IDE.
    *   Ensure your IDE has Maven support.
    *   Import the project as a Maven project.
    *   You can typically right-click on test classes (e.g., `KafkaTestContainersTest.java`, `PostgresTestContainersTest.java`) or the `src/test/java` directory and select "Run Tests".
    *   Remember that Docker must be running in the background.

### Running the Application

To run the Spring Boot application itself:

*   **Using Maven:**
    In your terminal, from the project root, execute:
    ```bash
    mvn spring-boot:run
    ```
    This will start the application. By default, it will try to connect to services (PostgreSQL, Kafka, AWS SNS/SQS) based on the configurations in `src/main/resources/application.yml`.

*   **Setting up Local Services (Optional - for local development without Testcontainers during app run):**
    If you want to run the application and have it connect to local instances of Kafka, PostgreSQL, etc., you can use the `docker-compose.yml` file located in the project root:
    1.  Ensure you are in the project root directory.
    2.  Run `docker-compose up -d`.
    This will start Kafka and PostgreSQL as defined in the compose file. The application, when started with `mvn spring-boot:run`, should then be able to connect to these services using the `localhost` addresses configured in `src/main/resources/application.yml`.

---
This project serves as a practical guide and example for implementing robust integration tests with Testcontainers.
