Rust kafka tutorial using `rdkafka`, from https://www.arroyo.dev/blog/using-kafka-with-rust.

To run Kafka locally, once downloaded, extracted and cd'd in the dir:

1. start the server

```bash
export KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
bin/kafka-server-start.sh config/kraft/server.properties
```

2. create a topic called `chat`

```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic chat --create
```