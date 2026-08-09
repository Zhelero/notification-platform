import json
import pytest

from consumer.decoding import decode_event


def test_decode_event_parses_valid_json():
    payload = json.dumps({
        "correlation_id": "abc",
        "user_id": 1,
        "channel": "email",
        "text": "hello",
    }).encode("utf-8")

    event = decode_event(payload)

    assert event == {
        "correlation_id": "abc",
        "user_id": 1,
        "channel": "email",
        "text": "hello",
    }


def test_decode_event_raises_value_error_on_non_json_bytes():
    with pytest.raises(ValueError):
        decode_event(b"this is not json")


def test_decode_event_raises_value_error_on_invalid_utf8():
    with pytest.raises(ValueError):
        decode_event(b"\xff\xfe\x00\x00")