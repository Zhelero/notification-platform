import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC = "notifications"

DB_CONFIG = {
    "user": "app",
    "password": "app",
    "database": "notifications",
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", "5433")),
}

ELASTICSEARCH_URL = os.getenv(
    "ELASTICSEARCH_URL",
    "http://localhost:9200",
)

LOG_FORMAT = (
    "%(asctime)s %(levelname)s %(name)s %(message)s "
    "%(correlation_id)s %(user_id)s %(channel)s %(offset)s"
)