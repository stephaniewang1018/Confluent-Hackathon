# Create a managed HTTP Sink connector (fastest default)

This file contains two easy ways to create an HTTP Sink connector that will deliver records from the `alerts` topic to a webhook: (A) using the Confluent Cloud CLI (ccloud) to create a *managed* connector, and (B) a cURL example for a self‑managed Kafka Connect REST API (if you run your own Connect worker). Use (A) for the fastest, recommended path in Confluent Cloud.

---

A) Managed connector with ccloud CLI (recommended)

1. Install and authenticate the ccloud CLI: https://docs.confluent.io/ccloud-cli/current/ccloud-cli/index.html

2. Prepare a connector configuration file (save as `connector_config.json`). Replace the placeholders with your webhook URL and the name you prefer.

connector_config.json
```
{
  "name": "http-sink-alerts",
  "connector.class": "com.confluent.connect.http.HttpSinkConnector",
  "tasks.max": "1",
  "topics": "alerts",
  "http.api.url": "https://your-webhook-or-service/endpoint",
  "http.method": "POST",
  "request.content.type": "application/json",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false"
}
```

Notes:
- For Confluent Cloud managed connectors you *do not* need to include `producer.override.*` auth properties — Confluent Cloud will run the connector within the managed environment and handle connectivity to your cluster.
- If you use Avro + Schema Registry for the `alerts` topic, set `value.converter` to `io.confluent.connect.avro.AvroConverter` and add the Schema Registry auth properties (see the README). For quick testing JSON is simplest.

3. Create the managed connector with ccloud CLI. You may need to specify the cluster/environment depending on your ccloud context. Example commands you can run after logging in:

# list your clusters, copy the cluster id
ccloud kafka cluster list

# create the connector (ccloud will use the active environment/cluster or you can pass flags)
ccloud connector create --config connector_config.json

If your ccloud CLI requires explicit flags for environment/cluster, refer to `ccloud connector create --help` or set the active environment with `ccloud environment use` and the active cluster with `ccloud kafka cluster use`.

After the command succeeds you should see the connector in the Confluent Cloud UI under Connectors → Connector name.

---

B) Self‑managed Kafka Connect (cURL to Connect REST API)

If you run your own Kafka Connect cluster (not the Confluent Cloud managed connectors), use this method. First save this `connector_payload.json` file (it includes SASL/SSL producer overrides so the Connect worker can talk to Confluent Cloud):

connector_payload.json
```
{
  "name": "http-sink-alerts",
  "config": {
    "connector.class": "org.apache.kafka.connect.http.HttpSinkConnector",
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

Then POST it to your Connect REST endpoint (replace `<CONNECT_HOST>`):

curl -X POST -H "Content-Type: application/json" --data @connector_payload.json http://<CONNECT_HOST>:8083/connectors

The Connect worker will create tasks and begin delivering alerts to your webhook endpoint.

---

Testing tip: Use https://webhook.site or https://requestbin.com to get a temporary webhook URL you can inspect. Create the connector pointing to that URL, run your Flink SQL job (which writes to `alerts`) and verify HTTP POSTs appear at the webhook.

If you want, I can add the `connector_config.json` and `connector_payload.json` files directly to the repo (with placeholders filled) so you can edit and run them locally. Would you like me to add those files now?