import json


def decode_event(raw: bytes) -> dict:
    """Decode a raw Kafka message value into an event dict.

    Deliberately NOT used as an AIOKafkaConsumer value_deserializer:
    a deserializer that raises breaks the `async for msg in consumer`
    iterator itself, outside any per-message try/except, which kills
    the whole consume loop on a single bad message. Calling this inside
    the loop's try/except lets one bad message be logged and skipped.
    """
    try:
        return json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Invalid message payload: {exc}") from exc