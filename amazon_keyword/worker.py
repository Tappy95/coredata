import pipeflow
import json
from sqlalchemy import create_engine
from sqlalchemy.sql import select,  update
from sqlalchemy.dialects.mysql import insert
from pipeflow import NsqInputEndpoint, NsqOutputEndpoint
from task_protocol import HYTask
from models.amazon_models import amazon_keyword_rank, amazon_keyword_result, amazon_keywrd_task
from config import *
from config import INPUT_NSQ_CONF, OUTPUT_NSQ_CONF
from api.amazon_keyword import AddAmazonKWM, DelAmazonKWM, GetAmazonKWMResult, GetAmazonKWMStatus

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
conn = engine.connect()


def handle(group, task):
    hy_task = HYTask(task)
    data = hy_task.data
    site = data.get('site')

    # 创建监控关键字排名api, 并把返回结果写入数据库记录下来
    cret_amaz_api = AddAmazonKWM(
        station=site,
        asin_and_keywords=data['asin_and_keywords'],
        num_of_days=data['num_of_days'],
        monitoring_num=data['monitoring_num']
    )
    station = {'station': site, 'is_ins': '0'}    # ins设为0 表示第一次添加到数据库
    creat_amaz_mon = cret_amaz_api.request()
    result = (creat_amaz_mon['result'])
    for i in result:
        result_dict = dict(i, **station)
        insert_stmt = insert(amazon_keyword_result).values(result_dict)
        on_conflict_stmt = insert_stmt.on_duplicate_key_update({"is_ins": station['is_ins']})
        conn.execute(on_conflict_stmt)


# 从amazon_keyword_result表取出id、station去调用show_asin_list API(获取用户监控的商品信息)获取结果写入数据库
def show_asin_list():
    result = conn.execute(select([amazon_keyword_result.c.id, amazon_keyword_result.c.station])
                          .where(amazon_keyword_result.c.is_ins == 0)).fetchall()

    # 已经查询过该商品 is_ins由0设为1
    resu_upd = conn.execute(
        update(amazon_keyword_result).values({"is_ins": 1}).where(amazon_keyword_result.c.is_ins == 0))

    if result != None:
        for i in result:
            asin_list = GetAmazonKWMStatus(
                ids=[i[0]],
                station=i[1],
                capture_status=3
            )
            asin_result = asin_list.request()

            for j in asin_result['result']['list']:
                for k in sta.keys():
                    if j['station'] == int(k):
                        j['station'] = sta[k]

            resu = asin_result['result']['list']
            for i in resu:
                insert_stmt = insert(amazon_keywrd_task).values(i)
                on_conflict_stmt = insert_stmt.on_duplicate_key_update(
                    {"created_at": i['created_at'], "deleted_at": i['deleted_at'],
                     "start_time": i['start_time'], "end_time": i['end_time']})
                conn.execute(on_conflict_stmt)


# 从amazon_keyword_task(任务调度表记录任务)取到参数调用 show_kw_asins(获取单个/批量当天的关键词排名监控纪录)API 获取排名记录数据库
def get_keyrank():
    result = conn.execute(select(
        [amazon_keywrd_task.c.id, amazon_keywrd_task.c.start_time, amazon_keywrd_task.c.end_time])).fetchall()
    for i in result:
        amaz_result = GetAmazonKWMResult(
            ids=[i[0]],
            start_time=i[1],
            end_time=i[2]
        )

        resu = (amaz_result.request())
        asin_list = resu['result'][0]['keyword_list']
        asin_list = json.loads(
            json.dumps(asin_list).replace('start_time', 'update_time').replace('station', 'site').replace(
                'keyword_rank', 'rank'))

        for j in asin_list:
            for k in sta.keys():
                if j['site'] == int(k):
                    j['site'] = sta[k]

        ins_task = conn.execute(amazon_keyword_rank.insert(), asin_list)


# 定期查看任务调度表的status状态判断是否失效(删除单个或多个关键词排名监控信息)
def del_mon_asins():
    dele = conn.execute(
        select([amazon_keywrd_task.c.status, amazon_keywrd_task.c.id]).where(
            (amazon_keywrd_task.c.status == -1) | (amazon_keywrd_task.c.status == -2)
        )).fetchall()
    for i in dele:
        del_fail = DelAmazonKWM(
            ids=[i[1]]
        )
        del_fail.request()
    conn.execute(
        amazon_keywrd_task.delete().where((amazon_keywrd_task.c.status == -2) | (amazon_keywrd_task.c.status == -1)))


def run():
    input_end = NsqInputEndpoint('haiying.amazon.keyword', 'haiying_crawler', WORKER_NUMBER, **INPUT_NSQ_CONF)
    output_end = NsqOutputEndpoint(**OUTPUT_NSQ_CONF)

    server = pipeflow.Server()
    group = server.add_group('main', WORKER_NUMBER)
    group.set_handle(handle, "thread")
    group.add_input_endpoint('input', input_end)
    group.add_output_endpoint('output', output_end, ('test',))

    server.add_routine_worker(show_asin_list, interval=60*3)
    server.add_routine_worker(get_keyrank, interval=60*24)
    server.add_routine_worker(del_mon_asins, interval=60*24*3)
    server.run()
