# Confluent Cloud + Flink SQL scaffold for Furniture Curb Drop Alerts

This scaffold provides a local Flink environment (JobManager + TaskManager + SQL client) via Docker Compose that connects to your Confluent Cloud Kafka cluster. It also includes a Java Haversine UDF, a Flink SQL example, and a mock Python producer to emit sample events.

Files added:
- docker-compose.yml: runs Flink JobManager, TaskManager, SQL client, and a mock producer.
- sql/furniture_alerts.sql: example Flink SQL job (uses a Haversine UDF).
- java-haversine/: a small Maven project with the Haversine ScalarFunction. Build and drop the resulting JAR into ./jars/.
- producer/: Python mock producer that sends JSON events to Confluent Cloud.
- .env.example: environment variable placeholders for Confluent Cloud credentials.

Quick start
1. Copy .env.example to .env and fill in your Confluent Cloud credentials.
2. Build the Java UDF JAR:
   cd java-haversine
   mvn package
   cp target/java-haversine-1.0-SNAPSHOT.jar ../jars/
3. Start the local Flink services:
   docker-compose up -d
4. Open the Flink SQL client container to run the SQL in sql/furniture_alerts.sql or use the web UI at http://localhost:8081

Note: This scaffold uses JSON for topics by default. For production, switch to Avro + Schema Registry and secure secrets via a secrets manager.
