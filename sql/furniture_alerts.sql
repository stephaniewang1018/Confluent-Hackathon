-- Register function after placing the Haversine JAR in Flink's classpath (./jars)
-- In the SQL client, run: CREATE TEMPORARY SYSTEM FUNCTION haversine AS 'com.example.Haversine';

-- Drops topic (events)
CREATE TABLE drops (
  event_id STRING,
  ts TIMESTAMP(3),
  lat DOUBLE,
  lon DOUBLE,
  item_type STRING,
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'drops',
  'properties.bootstrap.servers' = '${CONFLUENT_BOOTSTRAP_SERVERS}',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${CONFLUENT_API_KEY}" password="${CONFLUENT_API_SECRET}";',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- Subscriptions changelog (user locations)
CREATE TABLE subscriptions (
  user_id STRING,
  lat DOUBLE,
  lon DOUBLE,
  radius_miles DOUBLE,
  ts TIMESTAMP(3),
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'subscriptions',
  'properties.bootstrap.servers' = '${CONFLUENT_BOOTSTRAP_SERVERS}',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${CONFLUENT_API_KEY}" password="${CONFLUENT_API_SECRET}";',
  'format' = 'json'
);

CREATE TABLE alerts (
  user_id STRING,
  event_id STRING,
  alert_ts TIMESTAMP(3),
  distance_miles DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'alerts',
  'properties.bootstrap.servers' = '${CONFLUENT_BOOTSTRAP_SERVERS}',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${CONFLUENT_API_KEY}" password="${CONFLUENT_API_SECRET}";',
  'format' = 'json'
);

-- Insert alerts when within subscription radius (0.5 miles default stored in subscription events)
INSERT INTO alerts
SELECT s.user_id, d.event_id, d.ts AS alert_ts, haversine(d.lat, d.lon, s.lat, s.lon) AS distance_miles
FROM drops AS d
JOIN subscriptions FOR SYSTEM_TIME AS OF d.ts AS s
ON haversine(d.lat, d.lon, s.lat, s.lon) <= s.radius_miles;
