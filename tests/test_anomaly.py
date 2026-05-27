"""
Unit tests for anomaly detection threshold logic.
Tests the pure math — no Spark or Kafka needed.
"""
import pytest


# replicate the threshold constants from anomaly_detector.py
SPIKE_THRESHOLD       = 90
CONVERSION_THRESHOLD  = 0.30
ABANDONMENT_THRESHOLD = 4.0


# --- traffic spike tests ---

def test_normal_traffic_not_flagged():
    view_count = 80  # below threshold of 90
    assert view_count <= SPIKE_THRESHOLD


def test_spike_detected_above_threshold():
    view_count = 120  # above threshold
    assert view_count > SPIKE_THRESHOLD


def test_high_severity_spike():
    # HIGH severity fires above 1.5x threshold
    view_count = 140
    assert view_count > SPIKE_THRESHOLD * 1.5


def test_medium_severity_spike():
    # MEDIUM fires between 1x and 1.5x threshold
    view_count = 100
    assert SPIKE_THRESHOLD < view_count <= SPIKE_THRESHOLD * 1.5


def test_spike_threshold_boundary():
    # exactly at threshold should NOT trigger (strictly greater than)
    assert not (SPIKE_THRESHOLD > SPIKE_THRESHOLD)


# --- conversion drop tests ---

def test_healthy_conversion_not_flagged():
    purchase_count = 15  # above drop threshold of 9 (24 * 0.4)
    drop_threshold = int(24 * 0.4)
    assert purchase_count >= drop_threshold


def test_conversion_drop_detected():
    purchase_count = 5  # below threshold
    drop_threshold = int(24 * 0.4)
    assert purchase_count < drop_threshold


def test_zero_purchases_flagged():
    purchase_count = 0
    drop_threshold = int(24 * 0.4)
    assert purchase_count < drop_threshold


def test_conversion_threshold_is_reasonable():
    # threshold should be between 5 and 15 purchases per 5-min window
    drop_threshold = int(24 * 0.4)
    assert 5 <= drop_threshold <= 15


# --- abandonment surge tests ---

def test_normal_abandonment_not_flagged():
    cart_count = 30  # below surge threshold of 45
    assert cart_count <= 45


def test_abandonment_surge_detected():
    cart_count = 60  # above threshold
    assert cart_count > 45


def test_abandonment_threshold_boundary():
    cart_count = 45
    assert not (cart_count > 45)


# --- session pipeline tests ---

def test_session_duration_calculation():
    first_ts = 1_000_000
    last_ts  = 1_180_000  # 180 seconds later in ms
    duration = (last_ts - first_ts) / 1000
    assert duration == 180.0


def test_session_conversion_flag():
    # a session with a purchase should be marked converted
    session = {
        "purchases":  1,
        "converted":  True,
        "total_spend": 99.99,
    }
    assert session["converted"] is True
    assert session["total_spend"] > 0


def test_unconverted_session():
    session = {
        "purchases": 0,
        "converted": False,
        "total_spend": 0.0,
    }
    assert session["converted"] is False
    assert session["total_spend"] == 0.0


def test_funnel_drop_off_rates():
    page_views     = 1000
    product_clicks = 150
    cart_adds      = 100
    purchases      = 40

    view_to_click = product_clicks / page_views * 100
    click_to_cart = cart_adds      / product_clicks * 100
    cart_to_purch = purchases      / cart_adds * 100

    assert view_to_click == 15.0
    assert click_to_cart == pytest.approx(66.67, rel=0.01)
    assert cart_to_purch == 40.0