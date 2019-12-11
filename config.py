import os

PRODUCTION_ENV = True if os.environ.get('ENV_TYPE')=='PRODUCTION' else False


NSQ_LOOKUPD_HTTP_ADDR = 'bd-nsqlookupd:4161' if PRODUCTION_ENV else '134.73.133.2:25761'
NSQ_NSQD_TCP_ADDR = 'bd-nsqd:4150' if PRODUCTION_ENV else '134.73.133.2:25750'
NSQ_NSQD_HTTP_ADDR = 'bd-nsqd:4151' if PRODUCTION_ENV else '134.73.133.2:25751'

INPUT_NSQ_CONF = {
    'lookupd_http_addresses': [NSQ_LOOKUPD_HTTP_ADDR]
}
OUTPUT_NSQ_CONF = {
    'nsqd_tcp_addresses': NSQ_NSQD_TCP_ADDR
}


DB_USER_NAME = "root" if PRODUCTION_ENV else "linkcool"
DB_USER_PW = "@ie0bzy3!dlpq*d7" if PRODUCTION_ENV else "forconnect"
DB_SEVER_ADDR = "10.0.1.4:4000" if PRODUCTION_ENV else "119.145.69.74:43021"
DB_DATABASE_NAME = "bigdata"
SQLALCHEMY_DATABASE_URI = "mysql+pymysql://{name:s}:{pw:s}@{addr:s}/{db:s}".format(
    name=DB_USER_NAME,
    pw=DB_USER_PW,
    addr=DB_SEVER_ADDR,
    db=DB_DATABASE_NAME)
SQLALCHEMY_POOL_PRE_PING = True
SQLALCHEMY_ECHO = False if PRODUCTION_ENV else True
SQLALCHEMY_POOL_SIZE = 0
SQLALCHEMY_POOL_MAX_OVERFLOW = -1
SQLALCHEMY_POOL_RECYCLE = 120
