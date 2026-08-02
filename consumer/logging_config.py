import logging
import os

from pythonjsonlogger import jsonlogger
from elasticsearch import Elasticsearch

from elastic_handler import ElasticsearchHandler


def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s %(correlation_id)s %(user_id)s %(channel)s %(offset)s"
    ))
    logger.addHandler(console_handler)

    es = Elasticsearch(os.getenv("ELASTICSEARCH_URL", "http://localhost:9200"))
    logger.addHandler(ElasticsearchHandler(es))