from elasticsearch import Elasticsearch
import os
import time
import requests



EXPORTER_URL = os.environ['EXPORTER_URL']
# EXPORTER_URL = 'http://localhost/metric-reciever'

es = Elasticsearch( hosts=os.environ['ELASTICSEARCH_SCHEME'] + '://' +
                           os.environ['ELASTICSEARCH_HOST'] + ':' +
                           os.environ['ELASTICSEARCH_PORT'],
                           basic_auth=(os.environ['ELASTICSEARCH_USER'], os.environ['ELASTICSEARCH_PASSWORD']) )

hits_sended_errors = set()
hits_for_sending = []
BODY = {
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "level": "error"
          }
        },
        {"range": {
          "@timestamp": {
            "gte": "now-5m",
            "lte": "now"
          }
        }}
      ]
    }
  }
}

def get_new_errors_and_send_to_exporter():
    global hits_sended_errors
    global hits_for_sending
    es_query_response = es.search(index="*",body=BODY)
    hits = es_query_response['hits']['hits']
    hits_id = list(map(lambda x : x['_id'], hits ))
    # print('hits_id:' + str(hits_id))
    for item in hits_id:
        if item not in hits_sended_errors:
            hits_for_sending.append(item)
    send_metrics_to_exporter(hits_for_sending)
    hits_sended_errors.clear()
    hits_sended_errors = update_sended_errors(hits_sended_errors,hits_id)
    hits_for_sending = []


def send_metrics_to_exporter(hits_for_sending):
    data_len = len(hits_for_sending)
    data_json = { "errors" : data_len }
    app.logger.info(data)
    requests.post(EXPORTER_URL,json=data_json )

def update_sended_errors(sended_errors, hits_id):
    for i in hits_id:
        sended_errors.add(i)
    return  sended_errors

while True:
    try:
        get_new_errors_and_send_to_exporter()
    except Exception as e:
        app.logger.error(data)

    time.sleep(10)
