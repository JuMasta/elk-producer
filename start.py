from elastic_search_error_logs_supplier import get_new_errors_and_send_to_exporter
import time
import sys
import logging

log = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO)


args = {}
period = 10
log.info(sys.argv)
if len(sys.argv) > 1:
    for i in range(len(sys.argv)):
        if i != 0:
            item = sys.argv[i]
            key_value_array = item.split('=')
            key = key_value_array[0]
            value = key_value_array[1]
            args[key] = value
    period = int(args['period'])




while True:
    try:
        get_new_errors_and_send_to_exporter()
    except Exception as e:
        log.error(e)
    time.sleep(period)
