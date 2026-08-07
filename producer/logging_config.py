import logging
import os

from pythonjsonlogger.json import JsonFormatter
from elasticsearch import Elasticsearch

from producer.config import LOG_FORMAT, ELASTICSEARCH_URL
from producer.elastic_handler import ElasticsearchHandler


def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(JsonFormatter(LOG_FORMAT))
    logger.addHandler(console_handler)

    if os.getenv("ENABLE_ELASTIC_LOGGING", "false").lower() == "true":
        es = Elasticsearch(
            ELASTICSEARCH_URL,
            request_timeout=0.2,
            max_retries=0,
            retry_on_timeout=False,
        )
        logger.addHandler(ElasticsearchHandler(es))