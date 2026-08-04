import logging

from pythonjsonlogger.json import JsonFormatter
from elasticsearch import Elasticsearch

from consumer.elastic_handler import ElasticsearchHandler
from consumer.config import LOG_FORMAT, ELASTICSEARCH_URL


def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(JsonFormatter(LOG_FORMAT))
    logger.addHandler(console_handler)

    es = Elasticsearch(ELASTICSEARCH_URL)
    logger.addHandler(ElasticsearchHandler(es))