import os
from elasticsearch import Elasticsearch

es = Elasticsearch(os.getenv("ELASTICSEARCH_URL", "http://localhost:9200"))

def index_log(document: dict):
    es.index(index="notification-logs", document=document)