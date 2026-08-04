from unittest.mock import AsyncMock
from fastapi.testclient import TestClient

from producer.app import app

client = TestClient(app)

def test_notify_returns_correlation_id():
    mock_producer = AsyncMock()
    app.state.producer = mock_producer

    response = client.post(
        "/notify",
        json={
            "user_id": 1,
            "channel": "telegram",
            "text": "hello",
        },
    )

    assert response.status_code == 200

    data = response.json()

    assert data["status"] == "published"
    assert "correlation_id" in data

    mock_producer.send_and_wait.assert_called_once()

def test_notify_validation_error():
    response = client.post(
        "/notify",
        json={
            "user_id": 1,
            "channel": "telegram",
        },
    )

    assert response.status_code == 422