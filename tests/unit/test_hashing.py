from app.services.hashing import canonical_json_hash


def test_canonical_json_hash_is_stable_for_key_order() -> None:
    left = {"channel": "sms", "recipient_ids": [1, 2], "message": "hello"}
    right = {"message": "hello", "recipient_ids": [1, 2], "channel": "sms"}

    assert canonical_json_hash(left) == canonical_json_hash(right)

