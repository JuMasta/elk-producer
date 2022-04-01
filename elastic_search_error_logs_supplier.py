from elasticsearch import Elasticsearch
import os
import time
import requests
import logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
EXPORTER_URL = os.environ['EXPORTER_URL']
# EXPORTER_URL = 'http://localhost/metric-reciever'

es = Elasticsearch( hosts=os.environ['ELASTICSEARCH_SCHEME'] + '://' +
                           os.environ['ELASTICSEARCH_HOST'] + ':' +
                           os.environ['ELASTICSEARCH_PORT'],
                           basic_auth=(os.environ['ELASTICSEARCH_USER'], os.environ['ELASTICSEARCH_PASSWORD']), verify_certs=False )

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
    hits_id = list(map(lambda x :  { '_id':x['_id'], 'namespace_name' : x['_source']['kubernetes']['namespace_name'] , 'pod_name': x['_source']['kubernetes']['pod_name'] }, hits ))

    for item in hits_id:
        if item['_id'] not in hits_sended_errors:
            hits_for_sending.append(item)
    send_metrics_to_exporter(hits_for_sending)
    hits_sended_errors.clear()
    hits_sended_errors = update_sended_errors(hits_sended_errors,hits_id)
    hits_for_sending = []


def send_metrics_to_exporter(hits_for_sending):
    data_json = {
        'namespace_names': {}
    }

    for item in hits_for_sending:
        namespace_name = item['namespace_name']
        pod_name = item['pod_name']
        data_json['namespace_names'][namespace_name] = data_json['namespace_names'].get(namespace_name,{})
        namespace_object = data_json['namespace_names'][namespace_name]
        namespace_object[pod_name] = namespace_object.get(pod_name,0) + 1

    log.info(data_json)
    requests.post(EXPORTER_URL,json=data_json)

def update_sended_errors(sended_errors, hits_id):
    for i in hits_id:
        sended_errors.add(i['_id'])
    return  sended_errors

while True:
    try:
        get_new_errors_and_send_to_exporter()
    except Exception as e:
        log.error(e)

    time.sleep(10)
