import os

PRODUCTION_ENV = True if os.environ.get('ENV_TYPE')=='PRODUCTION' else False

NSQ_LOOKUPD_HTTP_ADDR = 'bigdata-nsqlookupd:4161' if PRODUCTION_ENV else '134.73.133.2:25761'
NSQ_NSQD_TCP_ADDR = 'bigdata-nsqd:4150' if PRODUCTION_ENV else '134.73.133.2:25750'
NSQ_NSQD_HTTP_ADDR = 'bigdata-nsqd:4151' if PRODUCTION_ENV else '134.73.133.2:25751'

INPUT_NSQ_CONF = {
    'lookupd_http_addresses': [NSQ_LOOKUPD_HTTP_ADDR]
}
OUTPUT_NSQ_CONF = {
    'nsqd_tcp_addresses': NSQ_NSQD_TCP_ADDR
}
