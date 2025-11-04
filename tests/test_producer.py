import json
from src.producer.generate_payments import random_transaction


def test_random_transaction_has_required_fields():
    evt = random_transaction()
    assert isinstance(evt, dict)
    required = {'transaction_id', 'ts_event', 'card_hash', 'merchant_id', 'amount', 'currency', 'auth_result'}
    assert required.issubset(set(evt.keys()))


def test_amount_is_positive():
    evt = random_transaction()
    assert evt['amount'] > 0
