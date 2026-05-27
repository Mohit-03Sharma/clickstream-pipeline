"""
Unit tests for the event simulator.
Tests event schema validation and funnel ratio logic.
No Kafka connection needed — pure logic tests.
"""
import uuid
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from producer.event_simulator import build_event, EVENT_WEIGHTS, TOPICS


def make_ids():
    return str(uuid.uuid4()), str(uuid.uuid4())


# --- schema tests ---

def test_page_view_has_required_fields():
    user_id, session_id = make_ids()
    event = build_event("page_view", user_id, session_id)
    assert "event_id"   in event
    assert "user_id"    in event
    assert "session_id" in event
    assert "page"       in event
    assert "device"     in event
    assert "timestamp"  in event


def test_product_click_has_price():
    user_id, session_id = make_ids()
    event = build_event("product_click", user_id, session_id)
    assert "price"      in event
    assert "product_id" in event
    assert "category"   in event
    assert event["price"] > 0


def test_add_to_cart_has_quantity():
    user_id, session_id = make_ids()
    event = build_event("add_to_cart", user_id, session_id)
    assert "quantity"   in event
    assert "cart_value" in event
    assert event["quantity"] >= 1


def test_purchase_has_order_id():
    user_id, session_id = make_ids()
    event = build_event("purchase", user_id, session_id)
    assert "order_id"    in event
    assert "total_value" in event
    assert "items_count" in event
    assert event["total_value"] > 0


def test_session_end_has_duration():
    user_id, session_id = make_ids()
    event = build_event("session_end", user_id, session_id)
    assert "duration_seconds" in event
    assert "pages_viewed"     in event
    assert event["duration_seconds"] >= 30


def test_user_id_preserved_in_event():
    user_id, session_id = make_ids()
    event = build_event("page_view", user_id, session_id)
    assert event["user_id"]    == user_id
    assert event["session_id"] == session_id


def test_timestamp_is_epoch_ms():
    user_id, session_id = make_ids()
    event = build_event("page_view", user_id, session_id)
    # epoch ms for 2026 is around 1.7 trillion
    assert event["timestamp"] > 1_700_000_000_000


def test_event_id_is_unique():
    user_id, session_id = make_ids()
    e1 = build_event("page_view", user_id, session_id)
    e2 = build_event("page_view", user_id, session_id)
    assert e1["event_id"] != e2["event_id"]


# --- funnel ratio tests ---

def test_event_weights_sum_to_one():
    total = sum(EVENT_WEIGHTS.values())
    assert abs(total - 1.0) < 0.0001


def test_pageviews_dominate_weights():
    # pageviews should be the largest single weight
    assert EVENT_WEIGHTS["page_view"] == max(EVENT_WEIGHTS.values())


def test_purchases_are_rarest():
    # purchases and sessions should be the rarest
    assert EVENT_WEIGHTS["purchase"] <= EVENT_WEIGHTS["add_to_cart"]
    assert EVENT_WEIGHTS["session_end"] <= EVENT_WEIGHTS["purchase"]


def test_all_event_types_have_topics():
    for event_type in EVENT_WEIGHTS:
        assert event_type in TOPICS, f"{event_type} missing from TOPICS"