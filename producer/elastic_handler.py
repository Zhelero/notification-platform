import logging
from datetime import datetime, UTC
from elasticsearch import Elasticsearch
from elastic_transport import ConnectionError


class ElasticsearchHandler(logging.Handler):
    def __init__(self, es_client: Elasticsearch, index: str = "notification-logs"):
        super().__init__()
        self.es = es_client
        self.index = index

    def emit(self, record):
        try:
            doc = {
                "@timestamp": datetime.now(UTC).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            for key in ("correlation_id", "user_id", "channel", "topic", "partition", "offset"):
                if hasattr(record, key):
                    doc[key] = getattr(record, key)

            self.es.index(index=self.index, document=doc)
        except ConnectionError:
            # Elasticsearch unavailable — do not interrupt app process.
            return
        except Exception:
            # Не роняем приложение, если Elasticsearch недоступен —
            # просто печатаем ошибку хендлера в stderr, как предписывает logging.Handler
            self.handleError(record)