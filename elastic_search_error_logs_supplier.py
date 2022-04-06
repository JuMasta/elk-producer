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
        { "term" : { "level.keyword" : "error" } },
        { "term" : { "level.keyword" : "warn" } },
        { "term" : { "level.keyword" : "warning" } }
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
    # hits_for_sending = [{'_id': 'pE3q_38BKpjbM2ZMLxvO', 'namespace_name': 'argocd', 'pod_name': 'argocd-repo-server-75ff6886fb-w7jln', 'level': 'error'},{'_id': 'pE3q_38BKpjbM2ZMLxvO2', 'namespace_name': 'argocd', 'pod_name': 'argocd-repo-server-75ff6886fb-w7jln', 'level': 'warn'}, {'_id': 'pk3q_38BKpjbM2ZMLxvO', 'namespace_name': 'argocd', 'pod_name': 'argocd-repo-server-75ff6886fb-w7jln', 'level': 'error'}, {'_id': 'q03q_38BKpjbM2ZMLxvO', 'namespace_name': 'argocd', 'pod_name': 'argocd-repo-server-75ff6886fb-w7jln', 'level': 'error'}, {'_id': 'rU3q_38BKpjbM2ZMLxvO', 'namespace_name': 'argocd', 'pod_name': 'argocd-repo-server-75ff6886fb-w7jln', 'level': 'error'}, {'_id': '1hnp_38Be8fj5AqrluSA', 'namespace_name': 'elastic-system', 'pod_name': 'elastic-operator-0', 'level': 'error'}, {'_id': 'WBnq_38Be8fj5AqrWOad', 'namespace_name': 'elastic-system', 'pod_name': 'elastic-exporter-69cf6b64d5-56flc', 'level': 'error'}, {'_id': 'WRnq_38Be8fj5AqrWOad', 'namespace_name': 'elastic-system', 'pod_name': 'elastic-exporter-69cf6b64d5-56flc', 'level': 'error'}, {'_id': 'Uhnq_38Be8fj5AqrP-aD', 'namespace_name': 'monitoring', 'pod_name': 'prometheus-operator-5f75d76f9f-v7rr7', 'level': 'warn'}, {'_id': 'M03q_38BKpjbM2ZMYh1p', 'namespace_name': 'monitoring', 'pod_name': 'prometheus-k8s-0', 'level': 'warn'}, {'_id': 't03r_38BKpjbM2ZMnB6l', 'namespace_name': 'monitoring', 'pod_name': 'prometheus-k8s-0', 'level': 'warn'}]
    for item in hits_for_sending:
        namespace_name = item[NAMESPACE_KEY]
        pod_name = item[POD_NAME_KEY]
        level_name = item[LEVEL_KEY]
        data_json['namespace_names'][namespace_name] = data_json['namespace_names'].get(namespace_name,{})
        namespace_object = data_json['namespace_names'][namespace_name]
        pod_object = namespace_object.get(pod_name, {})
        namespace_object[pod_name] = pod_object
        pod_object[level_name] = pod_object.get(level_name, 0) + 1

    log.warn(data_json)
    requests.post(EXPORTER_URL,json=data_json)


def update_sended_errors(sended_errors, hits_custom_format):
    for i in hits_custom_format:
        sended_errors.add(i[ID_KEY])
    return  sended_errors

def get_new_errors_and_send_to_exporter():
    global hits_sended_errors
    global hits_for_sending
    es_query_response = es.search(index="logstash-*",body=BODY, size=10000)
    hits = es_query_response['hits']['hits']
    log.warn('Длина: ' ,len(hits))
    hits_custom_format = create_custom_format(hits)
    filter_sended_hits(hits_custom_format)
    send_metrics_to_exporter(hits_for_sending)
    hits_sended_errors.clear()
    hits_sended_errors = update_sended_errors(hits_sended_errors, hits_custom_format)
    hits_for_sending = []
