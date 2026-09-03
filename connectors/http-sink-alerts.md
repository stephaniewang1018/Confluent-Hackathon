# Quick default: HTTP Sink for alerts (fastest/easiest)

Goal: deliver alerts from the `alerts` Kafka topic to an HTTP endpoint (webhook) with minimal setup. This is the fastest path for end-to-end testing because you only need a webhook receiver (e.g., https://webhook.site) — no DB or cloud storage required.

Two options: Confluent Cloud managed connector (recommended / easiest) or self-managed Kafka Connect (if you run your own Connect cluster).

1) Managed Confluent Cloud HTTP Sink (recommended)
- In Confluent Cloud UI: Connectors -> Add connector -> search for "HTTP Sink" (or "HTTP"), choose the managed HTTP Sink connector.
- Configure basic fields:
  - Connector name: `http-sink-alerts`
  - Topics: `alerts`
  - HTTP URL: `https://your-webhook-or-service/endpoint`
  - HTTP method: `POST`
  - Key/Value format: `JSON` (managed connector will typically accept JSON or Avro depending on your topic format)
  - Authentication: set if your webhook requires it (Bearer token / Basic auth)
- Click Launch. Confluent Cloud will run the connector and you can verify deliveries in the UI.

Why this is the fastest:
- No infra to configure beyond the webhook URL.
- No need to add AWS/DB credentials.
- Easy to inspect payloads with webhook.site for debugging.

2) Self-managed Kafka Connect HTTP Sink (REST API example)
- If you run your own Connect cluster, you can POST a connector config to the Connect REST API. Use the template below (replace placeholders):

```json
{
  "name": "http-sink-alerts",
  "config": {
    "connector.class": "org.apache.kafka.connect.rest.HttpSinkConnector",
    "tasks.max": "1",
    "topics": "alerts",
    "http.api.url": "https://your-webhook-or-service/endpoint",
    "http.method": "POST",
    "request.content.type": "application/json",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "producer.override.bootstrap.servers": "${CONFLUENT_BOOTSTRAP_SERVERS}",
    "producer.override.security.protocol": "SASL_SSL",
    "producer.override.sasl.mechanism": "PLAIN",
    "producer.override.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${CONFLUENT_API_KEY}\" password=\"${CONFLUENT_API_SECRET}\";"
  }
}
```

Notes for self-managed mode:
- You must install an HTTP Sink plugin (community plugin or Confluent Hub plugin) compatible with your Connect runtime.
- Include the producer.override.* SASL/Auth settings so the worker can talk to Confluent Cloud.
- If your alerts topic uses Avro + Schema Registry, change the value.converter to `io.confluent.connect.avro.AvroConverter` and add schema registry auth settings.

Quick test with webhook.site
1. Create a temporary endpoint at https://webhook.site and copy the URL.
2. Create a Confluent Cloud managed HTTP Sink connector pointing at that URL and topics=alerts.
3. Ensure your Flink SQL INSERT INTO alerts is running and producing records.
4. Inspect webhook.site; you should see HTTP POSTs with alert JSON payloads.

If you want, I can add an example connector creation script (cURL) or a step-by-step screenshot guide showing the Confluent Cloud UI flow. Which would you prefer?