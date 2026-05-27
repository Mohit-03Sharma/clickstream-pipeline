import json
import os
from datetime import datetime, timedelta
from collections import defaultdict

from airflow.decorators import dag, task

KAFKA_BOOTSTRAP     = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
OUTPUT_DIR          = "D:/clickstream-pipeline/dags/output"

DEFAULT_ARGS = {
    "owner": "mohit",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="clickstream_hourly_summary",
    description="Hourly batch summary of clickstream sessions and funnel",
    schedule="@hourly",
    start_date=datetime(2026, 5, 26),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["clickstream", "batch", "summary"],
)
def clickstream_hourly_summary():

    @task()
    def extract_events():
        """Read recent events from all Kafka topics using a Python consumer."""
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

        events = []
        empty_polls = 0

        # poll until we get 500 events or 10 consecutive empty polls
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
        print(f"Extracted {len(events)} events from Kafka")
        return events

    @task()
    def compute_sessions(events: list):
        """Group events by session_id and compute session-level metrics."""
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
            s = sessions[sid]
            s["user_id"]    = event.get("user_id")
            s["session_id"] = sid

            ts = event.get("timestamp", 0)
            s["first_event_ts"] = min(s["first_event_ts"], ts)
            s["last_event_ts"]  = max(s["last_event_ts"],  ts)

            topic = event.get("_topic", "")
            if "pageviews" in topic:
                s["page_views"] += 1
            elif "clicks" in topic:
                s["product_clicks"] += 1
            elif "cart" in topic:
                s["cart_adds"] += 1
            elif "purchases" in topic:
                s["purchases"]   += 1
                s["total_spend"] += event.get("total_value", 0)
                s["converted"]    = True

        # compute duration in seconds
        result = []
        for sid, s in sessions.items():
            if s["first_event_ts"] == float("inf"):
                continue
            duration = (s["last_event_ts"] - s["first_event_ts"]) / 1000
            result.append({
                **s,
                "duration_seconds": round(duration, 1),
                "first_event_ts":   s["first_event_ts"],
            })

        print(f"Computed {len(result)} session summaries")
        return result

    @task()
    def compute_funnel(events: list):
        """Compute hourly funnel drop-off rates."""
        counts = {
            "page_views":     0,
            "product_clicks": 0,
            "cart_adds":      0,
            "purchases":      0,
        }

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

        # drop-off rates: what % of the previous stage made it to the next
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

        print(f"Funnel: {funnel}")
        return funnel

    @task()
    def write_summaries(sessions: list, funnel: dict):
        """Write session summaries and funnel to JSON files."""
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        sessions_path = f"{OUTPUT_DIR}/sessions_{ts}.json"
        funnel_path   = f"{OUTPUT_DIR}/funnel_{ts}.json"

        with open(sessions_path, "w") as f:
            json.dump(sessions, f, indent=2)

        with open(funnel_path, "w") as f:
            json.dump(funnel, f, indent=2)

        print(f"Wrote {len(sessions)} sessions to {sessions_path}")
        print(f"Wrote funnel to {funnel_path}")
        return {"sessions_file": sessions_path, "funnel_file": funnel_path}

    @task()
    def data_quality_check(write_result: dict, sessions: list, funnel: dict):
        """Basic quality checks — fail the task if something looks wrong."""
        errors = []

        if len(sessions) == 0:
            errors.append("No sessions computed — pipeline may be broken")

        if funnel["page_views"] == 0:
            errors.append("Zero page views in funnel — no data extracted")

        if funnel["overall_conversion"] > 50:
            errors.append(f"Conversion rate {funnel['overall_conversion']}% seems too high — check data")

        converted = sum(1 for s in sessions if s["converted"])
        print(f"Quality check: {len(sessions)} sessions, {converted} converted, funnel={funnel}")

        if errors:
            raise ValueError(f"Data quality checks failed: {errors}")

        print("All quality checks passed.")
        return True

    # wire the tasks together
    events   = extract_events()
    sessions = compute_sessions(events)
    funnel   = compute_funnel(events)
    written  = write_summaries(sessions, funnel)
    data_quality_check(written, sessions, funnel)


clickstream_hourly_summary()