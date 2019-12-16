import pipeflow
import json
from sqlalchemy import create_engine
from sqlalchemy.sql import select,  update
from sqlalchemy.dialects.mysql import insert
from pipeflow import NsqInputEndpoint, NsqOutputEndpoint
from task_protocol import HYTask
from models.amazon_models import amazon_keyword_rank, amazon_keyword_task
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

sta = {1: 'US', 2: 'IT', 3: 'JP', 4: 'DE', 5: 'UK', 6: 'FR', 7: 'ES', 8: 'CA', 9: 'AU'}


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
    station = {'station': site}
    creat_amaz_mon = cret_amaz_api.request()
    result = (creat_amaz_mon['result'])
    with engine.connect() as conn:
        for c_result in result:
            result_dict = dict(c_result, **station)
            insert_stmt = insert(amazon_keyword_task).values(id=result_dict['id'], station=result_dict['station'], is_add=0)
            on_conflict_stmt = insert_stmt.on_duplicate_key_update({"phone_num": None})
            conn.execute(on_conflict_stmt)


class TaskModel:
    def __init__(self):
        self.conn = engine.connect()

    def __del__(self):
        self.conn.close()

    def task_result(self):
        task = self.conn.execute(select([amazon_keyword_task.c.id, amazon_keyword_task.c.station,
                                         amazon_keyword_task.c.start_time, amazon_keyword_task.c.end_time])).fetchall()
        return task


class AmazonTask(TaskModel):

    def show_asin_list(self):
        results = self.conn.execute(select([amazon_keyword_task.c.id, amazon_keyword_task.c.station])
                          .where(amazon_keyword_task.c.is_add==0)).fetchall()

        self.conn.execute(update(amazon_keyword_task).values({"is_add": 1}).where(amazon_keyword_task.c.is_add == 0))

        for result in results:
            kws_sta = GetAmazonKWMStatus(
                ids=[result[0]],
                station=result[1],
                capture_status=1
            )
            kws_result = kws_sta.request()

            for j in kws_result['result']['list']:
                for k in sta.keys():
                    if j['station'] == int(k):
                        j['station'] = sta[k]

            resu = kws_result['result']['list']

            for i in resu:
                insert_stmt = insert(amazon_keyword_task).values(i)
                on_conflict_stmt = insert_stmt.on_duplicate_key_update(
                    {"capture_status": i['capture_status'], "status": i['status'],
                     "monitoring_num": i['monitoring_num'], "monitoring_count": i['monitoring_count'],
                     "start_time": i['start_time'], "end_time": i['end_time'],
                     "monitoring_type": i['monitoring_type'], "created_at": i['created_at'],
                     "deleted_at": i['deleted_at'], "phone_num": i['phone_num'], "asin": i['asin'],
                     "keyword": i['keyword']})
                self.conn.execute(on_conflict_stmt)

    # 从amazon_keyword_task(任务调度表记录任务)取到参数调用 show_kw_asins(获取单个/批量当天的关键词排名监控纪录)API 获取排名记录数据库
    def get_keyword_rank(self):
            for i in self.task_result():
                amaz_result = GetAmazonKWMResult(
                    ids=[i[0]],
                    start_time=i[2],
                    end_time=i[3]
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

                ins_task = self.conn.execute(amazon_keyword_rank.insert(), asin_list)

    # 定期查看任务调度表的status状态判断是否失效(删除单个或多个关键词排名监控信息)
    def del_mon(self):
            dele = self.conn.execute(
                select([amazon_keyword_task.c.status, amazon_keyword_task.c.id]).where(
                    (amazon_keyword_task.c.status == -1) | (amazon_keyword_task.c.status == -2)
                )).fetchall()

            for i in dele:
                del_fail = DelAmazonKWM(
                    ids=[i[1]]
                )
                del_fail.request()
            self.conn.execute(
                amazon_keyword_task.delete().where((amazon_keyword_task.c.status == -2) | (amazon_keyword_task.c.status == -1)))

def run():
    input_end = NsqInputEndpoint('haiying.amazon.keyword', 'haiying_crawler', WORKER_NUMBER, **INPUT_NSQ_CONF)
    output_end = NsqOutputEndpoint(**OUTPUT_NSQ_CONF)

    Task = AmazonTask()

    server = pipeflow.Server()
    group = server.add_group('main', WORKER_NUMBER)
    group.set_handle(handle, "thread")
    group.add_input_endpoint('input', input_end)
    group.add_output_endpoint('output', output_end, ('test',))

    server.add_routine_worker(Task.show_asin_list, interval=60*3)
    server.add_routine_worker(Task.get_keyword_rank, interval=60*24)
    server.add_routine_worker(Task.del_mon, interval=60*24*3)

    server.run()
