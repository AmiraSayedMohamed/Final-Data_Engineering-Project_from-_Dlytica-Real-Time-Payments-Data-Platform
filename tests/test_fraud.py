from datetime import datetime, timedelta
from src.spark_stream.fraud import high_amount, velocity_exceeds, cross_border, decline_rate


def test_high_amount():
    assert high_amount(15000, threshold=10000)
    assert not high_amount(9999, threshold=10000)


def test_velocity_exceeds():
    events = [1, 2, 3, 4, 5, 6]
    assert velocity_exceeds(events, limit=5)
    assert not velocity_exceeds([1, 2, 3], limit=5)


def test_cross_border():
    now = datetime.utcnow()
    last = ('USA', now - timedelta(minutes=5))
    assert cross_border(last, 'CAN', now, minutes=10)
    # outside window
    assert not cross_border(last, 'CAN', now + timedelta(minutes=20), minutes=10)
    # same country
    assert not cross_border(last, 'USA', now, minutes=10)


def test_decline_rate():
    w = ['APPROVED', 'DECLINED', 'DECLINED', 'DECLINED']
    assert decline_rate(w, threshold=0.6)
    assert not decline_rate(w, threshold=0.8)
import pytest
from src.spark_stream.fraud import high_amount, velocity_exceeds, cross_border, decline_rate
from datetime import datetime, timedelta


def test_high_amount():
    assert high_amount(15000)
    assert not high_amount(100)


def test_velocity():
    events = [1,2,3,4,5,6]
    assert velocity_exceeds(events, limit=5)
    assert not velocity_exceeds([1,2], limit=5)


def test_cross_border():
    now = datetime.utcnow()
    last = ('USA', now - timedelta(minutes=5))
    assert cross_border(last, 'CAN', now, minutes=10)
    assert not cross_border(last, 'USA', now, minutes=10)


def test_decline_rate():
    assert decline_rate(['APPROVED','DECLINED','DECLINED','DECLINED','DECLINED','DECLINED','DECLINED','DECLINED','DECLINED','DECLINED'], threshold=0.5)
    assert not decline_rate(['APPROVED','APPROVED','DECLINED'], threshold=0.5)
