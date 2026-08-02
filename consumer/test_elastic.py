from datetime import datetime, UTC
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

doc = {
    "@timestamp": datetime.now(UTC).isoformat(),
    "message": "Test log",
    "correlation_id": "test-123",
    "user_id": 1,
    "channel": "telegram",
}

response = es.index(index="notification-logs", document=doc)

print(response)