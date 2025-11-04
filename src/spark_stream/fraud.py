"""Fraud rule helper functions used by streaming and unit tests."""
from collections import deque
from datetime import datetime, timedelta, timezone


def high_amount(amount, threshold=10000.0):
    return amount is not None and amount > threshold


def velocity_exceeds(window_events, limit=5):
    """window_events: list of event timestamps (datetime) in the window for a card"""
    return len(window_events) > limit


def cross_border(last_country_ts, current_country, now_ts, minutes=10):
    """last_country_ts: tuple(last_country, last_ts)
       current_country: country string
       now_ts: datetime of current event"""
    if not last_country_ts:
        return False
    last_country, last_ts = last_country_ts
    if last_country != current_country and (now_ts - last_ts) <= timedelta(minutes=minutes):
        return True
    return False


def decline_rate(declines_window, threshold=0.5):
    """declines_window: deque/list of last N auth_result strings"""
    if not declines_window:
        return False
    declines = sum(1 for r in declines_window if r == 'DECLINED')
    return (declines / len(declines_window)) > threshold
