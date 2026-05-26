from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import json

KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

# read from pageviews — the highest volume topic
TOPIC = "clickstream.pageviews"

sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
deserializer = AvroDeserializer(sr_client)  # fetches schema by ID from each message

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "verify-consumer-group",
    "auto.offset.reset": "earliest",  # start from the beginning of the topic
})

consumer.subscribe([TOPIC])

print(f"Reading from {TOPIC}... (Ctrl+C to stop)\n")

count = 0
try:
    while True:
        msg = consumer.poll(timeout=1.0)  # wait up to 1 second for a message
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        event = deserializer(msg.value(), SerializationContext(TOPIC, MessageField.VALUE))
        print(json.dumps(event, indent=2))
        count += 1
        if count >= 5:  # print 5 events then stop
            break
finally:
    consumer.close()
    print(f"\nRead {count} events successfully.")