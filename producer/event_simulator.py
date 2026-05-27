import json
import time
import random
import uuid
from datetime import datetime
from faker import Faker

fake = Faker()

# --- Config ---
KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
EVENTS_PER_SECOND = 10

TOPICS = {
    "page_view":     "clickstream.pageviews",
    "product_click": "clickstream.clicks",
    "add_to_cart":   "clickstream.cart",
    "purchase":      "clickstream.purchases",
    "session_end":   "clickstream.sessions",
}

EVENT_WEIGHTS = {
    "page_view":     0.70,
    "product_click": 0.15,
    "add_to_cart":   0.10,
    "purchase":      0.04,
    "session_end":   0.01,
}

PAGES      = ["/home", "/products", "/search", "/product/detail", "/cart", "/checkout", "/account"]
CATEGORIES = ["electronics", "clothing", "books", "home", "sports", "beauty", "toys"]
DEVICES    = ["mobile", "desktop", "tablet"]


def load_schema(event_type):
    path = f"producer/schemas/{event_type}.avsc"
    with open(path) as f:
        return json.load(f)


def register_schema(event_type, schema_dict):
    import requests  # local import — not needed at module level
    topic   = TOPICS[event_type]
    subject = f"{topic}-value"
    url     = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    payload  = {"schema": json.dumps(schema_dict)}
    response = requests.post(url, json=payload, headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
    response.raise_for_status()
    schema_id = response.json()["id"]
    print(f"Registered schema for {subject} → schema ID {schema_id}")
    return schema_id


def make_serializer(schema_dict):
    # local imports — Kafka machinery only needed when actually producing
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    sr_client  = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    schema_str = json.dumps(schema_dict)
    return AvroSerializer(sr_client, schema_str)


def build_event(event_type, user_id, session_id):
    base = {
        "event_id":   str(uuid.uuid4()),
        "user_id":    user_id,
        "session_id": session_id,
        "timestamp":  int(datetime.utcnow().timestamp() * 1000),
    }

    if event_type == "page_view":
        return {**base, "page": random.choice(PAGES), "device": random.choice(DEVICES)}

    if event_type == "product_click":
        return {**base,
                "product_id": str(uuid.uuid4()),
                "category":   random.choice(CATEGORIES),
                "price":      round(random.uniform(5.0, 500.0), 2)}

    if event_type == "add_to_cart":
        return {**base,
                "product_id": str(uuid.uuid4()),
                "quantity":   random.randint(1, 5),
                "cart_value": round(random.uniform(10.0, 800.0), 2)}

    if event_type == "purchase":
        return {**base,
                "order_id":    str(uuid.uuid4()),
                "total_value": round(random.uniform(20.0, 1000.0), 2),
                "items_count": random.randint(1, 8)}

    if event_type == "session_end":
        return {**base,
                "duration_seconds": random.randint(30, 1800),
                "pages_viewed":     random.randint(1, 20)}


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")


def main():
    from confluent_kafka import Producer
    from confluent_kafka.serialization import SerializationContext, MessageField

    print("Loading schemas...")
    schemas = {et: load_schema(et) for et in TOPICS}

    print("Registering schemas with Schema Registry...")
    for event_type, schema_dict in schemas.items():
        register_schema(event_type, schema_dict)

    print("Building serializers...")
    serializers = {et: make_serializer(schema_dict) for et, schema_dict in schemas.items()}

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    user_pool    = [str(uuid.uuid4()) for _ in range(50)]
    session_pool = {u: str(uuid.uuid4()) for u in user_pool}

    event_types = list(EVENT_WEIGHTS.keys())
    weights     = list(EVENT_WEIGHTS.values())

    print(f"\nStreaming {EVENTS_PER_SECOND} events/sec. Ctrl+C to stop.\n")

    count = 0
    while True:
        event_type = random.choices(event_types, weights=weights, k=1)[0]
        user_id    = random.choice(user_pool)
        session_id = session_pool[user_id]

        event = build_event(event_type, user_id, session_id)
        topic = TOPICS[event_type]

        if random.random() < 0.002:
            session_pool[user_id] = str(uuid.uuid4())

        producer.produce(
            topic=topic,
            value=serializers[event_type](event, SerializationContext(topic, MessageField.VALUE)),
            on_delivery=delivery_report,
        )

        count += 1
        if count % 100 == 0:
            producer.flush()
            print(f"Sent {count} events")

        time.sleep(1 / EVENTS_PER_SECOND)


if __name__ == "__main__":
    main()