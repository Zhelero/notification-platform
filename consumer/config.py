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