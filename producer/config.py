import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC = "notifications"

ELASTICSEARCH_URL = os.getenv(
    "ELASTICSEARCH_URL",
    "http://localhost:9200",
)

LOG_FORMAT = (
    "%(asctime)s %(levelname)s %(name)s %(message)s "
    "%(correlation_id)s %(user_id)s %(channel)s %(offset)s"
)