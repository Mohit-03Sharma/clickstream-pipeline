"""
Standalone runner for the hourly summary pipeline.
Mirrors the Airflow DAG task sequence exactly — same logic, no daemon required.
On Linux/Mac this would run via: airflow dags trigger clickstream_hourly_summary
"""
import json
import os
from datetime import datetime
from collections import defaultdict

KAFKA_BOOTSTRAP     = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
OUTPUT_DIR          = "D:/clickstream-pipeline/dags/output"


def extract_events():
    from confluent_kafka import Consumer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import SerializationContext, MessageField

    sr_client    = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(sr_client)

    topics = [
        "clickstream.pageviews",
        "clickstream.clicks",
        "clickstream.cart",
        "clickstream.purchases",
        "clickstream.sessions",
    ]

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id":          "airflow-hourly-summary",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(topics)

    events      = []
    empty_polls = 0

    while len(events) < 500 and empty_polls < 10:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            continue
        empty_polls = 0
        try:
            event = deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE)
            )
            event["_topic"] = msg.topic()
            events.append(event)
        except Exception:
            continue

    consumer.close()
    print(f"[extract_events] Extracted {len(events)} events")
    return events


def compute_sessions(events):
    sessions = defaultdict(lambda: {
        "user_id":        None,
        "session_id":     None,
        "first_event_ts": float("inf"),
        "last_event_ts":  float("-inf"),
        "page_views":     0,
        "product_clicks": 0,
        "cart_adds":      0,
        "purchases":      0,
        "total_spend":    0.0,
        "converted":      False,
    })

    for event in events:
        sid = event.get("session_id")
        if not sid:
            continue
        s            = sessions[sid]
        s["user_id"] = event.get("user_id")
        s["session_id"] = sid

        ts = event.get("timestamp", 0)
        s["first_event_ts"] = min(s["first_event_ts"], ts)
        s["last_event_ts"]  = max(s["last_event_ts"],  ts)

        topic = event.get("_topic", "")
        if "pageviews" in topic:
            s["page_views"]     += 1
        elif "clicks" in topic:
            s["product_clicks"] += 1
        elif "cart" in topic:
            s["cart_adds"]      += 1
        elif "purchases" in topic:
            s["purchases"]      += 1
            s["total_spend"]    += event.get("total_value", 0)
            s["converted"]       = True

    result = []
    for sid, s in sessions.items():
        if s["first_event_ts"] == float("inf"):
            continue
        duration = (s["last_event_ts"] - s["first_event_ts"]) / 1000
        result.append({**s, "duration_seconds": round(duration, 1)})

    print(f"[compute_sessions] {len(result)} sessions computed")
    return result


def compute_funnel(events):
    counts = {"page_views": 0, "product_clicks": 0, "cart_adds": 0, "purchases": 0}

    for event in events:
        topic = event.get("_topic", "")
        if "pageviews" in topic:
            counts["page_views"]     += 1
        elif "clicks" in topic:
            counts["product_clicks"] += 1
        elif "cart" in topic:
            counts["cart_adds"]      += 1
        elif "purchases" in topic:
            counts["purchases"]      += 1

    pv = counts["page_views"]     or 1
    pc = counts["product_clicks"] or 1
    ca = counts["cart_adds"]      or 1

    funnel = {
        **counts,
        "view_to_click_rate":    round(counts["product_clicks"] / pv * 100, 1),
        "click_to_cart_rate":    round(counts["cart_adds"]      / pc * 100, 1),
        "cart_to_purchase_rate": round(counts["purchases"]       / ca * 100, 1),
        "overall_conversion":    round(counts["purchases"]       / pv * 100, 2),
        "computed_at":           datetime.utcnow().isoformat(),
    }

    print(f"[compute_funnel] {funnel}")
    return funnel


def write_summaries(sessions, funnel):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    sessions_path = f"{OUTPUT_DIR}/sessions_{ts}.json"
    funnel_path   = f"{OUTPUT_DIR}/funnel_{ts}.json"

    with open(sessions_path, "w") as f:
        json.dump(sessions, f, indent=2)
    with open(funnel_path, "w") as f:
        json.dump(funnel, f, indent=2)

    print(f"[write_summaries] Sessions → {sessions_path}")
    print(f"[write_summaries] Funnel   → {funnel_path}")
    return sessions_path, funnel_path


def data_quality_check(sessions, funnel):
    errors = []

    if len(sessions) == 0:
        errors.append("No sessions computed")
    if funnel["page_views"] == 0:
        errors.append("Zero page views")
    if funnel["overall_conversion"] > 50:
        errors.append(f"Conversion {funnel['overall_conversion']}% too high")

    converted = sum(1 for s in sessions if s["converted"])
    print(f"[quality_check] {len(sessions)} sessions, {converted} converted")

    if errors:
        raise ValueError(f"Quality checks failed: {errors}")

    print("[quality_check] All checks passed.")


def main():
    print("=" * 50)
    print(f"Hourly summary run: {datetime.utcnow().isoformat()}")
    print("=" * 50)

    # task 1
    events = extract_events()

    # task 2 and 3 run independently (parallel in Airflow, sequential here)
    sessions = compute_sessions(events)
    funnel   = compute_funnel(events)

    # task 4
    write_summaries(sessions, funnel)

    # task 5
    data_quality_check(sessions, funnel)

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()