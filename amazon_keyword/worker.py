import pipeflow
import  json
from sqlalchemy import create_engine
from sqlalchemy.sql import select, and_, bindparam
from pipeflow import NsqInputEndpoint, NsqOutputEndpoint
from task_protocol import HYTask
from models.amazon_models import amazon_keyword_rank
from config import *
from config import INPUT_NSQ_CONF, OUTPUT_NSQ_CONF
from api.amazon_keyword import AddAmazonKWM, DelAmazonKWM, GetAmazonKWMAllResult, GetAmazonKWMResult, GetAmazonKWMStatus


WORKER_NUMBER = 2

engine = create_engine(
    SQLALCHEMY_DATABASE_URI,
    pool_pre_ping=SQLALCHEMY_POOL_PRE_PING,
    echo=SQLALCHEMY_ECHO,
    pool_size=SQLALCHEMY_POOL_SIZE,
    max_overflow=SQLALCHEMY_POOL_MAX_OVERFLOW,
    pool_recycle=SQLALCHEMY_POOL_RECYCLE,
)

sta = {1: 'us', 2: 'it', 3: 'jp', 4: 'de', 5: 'uk', 6: 'fr', 7: 'es', 8: 'ca', 9: 'au'}


def handle(group, task):
    hy_task = HYTask(task)
    data = hy_task.data
    site = data.get('site')

    # 创建监控关键字排名api
    cret_amaz_api = AddAmazonKWM(
        station=site,
        asin_and_keywords=data['asin_and_keywords'],
        num_of_days=data['num_of_days'],
        monitoring_num=data['monitoring_num']
    )
    creat_amaz_mon = cret_amaz_api.request()

    add_amaz_mysql(data=data)


# 获取当天用户所有关键词排名的监控纪录(写入数据库)
def add_amaz_mysql(data):

    amaz_result = GetAmazonKWMAllResult(
        station=data['site']
    )

    show_amaz = amaz_result.request()

    asin_list = []
    [asin_list.append(show_amaz['result'][x]['keyword_list'][-1]) for x in range(len(show_amaz['result']) -1)]

    for i in asin_list:
        for k in sta.keys():
            if i['station'] == int(k):
                i['station'] = sta[k]

    asin_list = json.loads(
        json.dumps(asin_list).replace('start_time', 'update_time').replace('station', 'site').replace('keyword_rank',
                                                                                                      'rank'))
    conn = engine.connect()
    conn.execute(amazon_keyword_rank.insert(), asin_list)


# 删除单个或多个关键词排名监控信息
def del_aws_kw(ids):
    del_mon = DelAmazonKWM(
            ids=ids
            )
    del_mon.request()


# 获取用户监控的商品信息
def show_asin_list():
    GetAmazonKWMResult(
        ids = ''
    )


# 获取单个/批量当天的关键词排名监控纪录(默认当天)
def show_kw_asins():
    GetAmazonKWMStatus(
        station = '',
        ids = ''
    )


def run():
    input_end = NsqInputEndpoint('haiying.amazon.keyword', 'haiying_crawler', WORKER_NUMBER,  **INPUT_NSQ_CONF)
    output_end = NsqOutputEndpoint(**OUTPUT_NSQ_CONF)

    server = pipeflow.Server()
    group = server.add_group('main', WORKER_NUMBER)
    group.set_handle(handle, "thread")
    group.add_input_endpoint('input', input_end)
    group.add_output_endpoint('output', output_end, ('test',))

    server.add_routine_worker(add_amaz_mysql, interval=60*60*24)
    server.run()

