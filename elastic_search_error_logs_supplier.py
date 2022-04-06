from elasticsearch import Elasticsearch
import os
import requests
import logging
log = logging.getLogger(__name__)

EXPORTER_URL = os.environ['EXPORTER_URL']

host = os.environ['ELASTICSEARCH_SCHEME'] + '://' + os.environ['ELASTICSEARCH_HOST'] + ':' + os.environ['ELASTICSEARCH_PORT']
es = Elasticsearch( hosts= host, basic_auth=(os.environ['ELASTICSEARCH_USER'], os.environ['ELASTICSEARCH_PASSWORD']), verify_certs=False )

hits_sended_errors = set()
hits_for_sending = []


# key names whcih uses in request object for metric exporter
NAMESPACE_KEY = 'namespace_name'
POD_NAME_KEY = 'pod_name'
LEVEL_KEY = 'level'
ID_KEY = '_id'


BODY = { "query": {
    "bool": {
      "should" : [
        { "term" : { "level" : "error" } },
        { "term" : { "level" : "warn" } }
      ],
      "filter": [
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




def create_custom_format(hits):
    hits_custom_format = list(map(lambda x :  { ID_KEY:x['_id'], NAMESPACE_KEY : x['_source']['kubernetes']['namespace_name'] , POD_NAME_KEY: x['_source']['kubernetes']['pod_name'] , LEVEL_KEY : x['_source']['level']}, hits ))
    return hits_custom_format

def filter_sended_hits(hits_custom_format):
    for item in hits_custom_format:
        if item[ID_KEY] not in hits_sended_errors:
            hits_for_sending.append(item)

def send_metrics_to_exporter(hits_for_sending):
    data_json = {
        'namespace_names': {}
    }
    log.info(hits_for_sending)
    for item in hits_for_sending:
        namespace_name = item[NAMESPACE_KEY]
        pod_name = item[POD_NAME_KEY]
        level_name = item[LEVEL_KEY]
        data_json['namespace_names'][namespace_name] = data_json['namespace_names'].get(namespace_name,{})
        namespace_object = data_json['namespace_names'][namespace_name]
        pod_object = namespace_object.get(pod_name, {})
        pod_object[level_name] = pod_object.get(level_name,0) + 1
        namespace_object[pod_name] = pod_object[level_name]

    log.info(data_json)
    requests.post(EXPORTER_URL,json=data_json)


def update_sended_errors(sended_errors, hits_custom_format):
    for i in hits_custom_format:
        sended_errors.add(i[ID_KEY])
    return  sended_errors

def get_new_errors_and_send_to_exporter():
    global hits_sended_errors
    global hits_for_sending
    es_query_response = es.search(index="*",body=BODY)
    hits = es_query_response['hits']['hits']
    hits_custom_format = create_custom_format(hits)
    filter_sended_hits(hits_custom_format)
    send_metrics_to_exporter(hits_for_sending)
    hits_sended_errors.clear()
    hits_sended_errors = update_sended_errors(hits_sended_errors, hits_custom_format)
    hits_for_sending = []



{'namespace_names': {
    "elastic_system": {
    "pod_name": {
        "warn": 1,
        "error" : 2
    }
    }
}}
