import pipeflow
import json
import time
from sqlalchemy import create_engine
from sqlalchemy.sql import select, update, and_
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


# 返回当前时间
def time_now():
    now_time = time.strftime("%Y-%m-%d %H:%M:%S")
    return now_time


def handle(group, task):
    hy_task = HYTask(task)
    data = hy_task.data
    site = data.get('site')
    res = data['asin_and_keywords']

    with engine.connect() as conn:
        for i in res:
            asin = i['asin']
            keyword = i['keyword']
            sql_result = conn.execute(select([amazon_keyword_task]).where(
                and_(amazon_keyword_task.c.asin == asin, amazon_keyword_task.c.keyword == keyword,
                     amazon_keyword_task.c.station == site))).first()

            # 数据库查询无 创建监控api
            if sql_result == None:
                asin = asin
                keyword = keyword
                asin_keyword = [{"asin": asin, "keyword": keyword}]

                create_amazon_api = AddAmazonKWM(
                    station=site,
                    asin_and_keywords=asin_keyword,
                    num_of_days=data['num_of_days'],
                    monitoring_num=data['monitoring_num']
                )
                station = {'station': site}
                create_amazon_mon = create_amazon_api.request()
                result = (create_amazon_mon['result'])

                for c_result in result:
                    result_dict = dict(c_result, **station)
                    insert_stmt = insert(amazon_keyword_task).values(id=result_dict['id'],
                                                                     station=result_dict['station'], is_add=0,
                                                                     asin=result_dict['asin'],
                                                                     keyword=result_dict['keyword'],
                                                                     last_update=time_now())
                    on_conflict_stmt = insert_stmt.on_duplicate_key_update({"last_update": time_now()})
                    conn.execute(on_conflict_stmt)

            # 数据库查询有  执行update更新last_update为当前时间
            else:
                conn.execute(
                    update(amazon_keyword_task).values({"last_update": time_now()}).where(
                        and_(amazon_keyword_task.c.asin == asin, amazon_keyword_task.c.station == site,
                             amazon_keyword_task.c.keyword == keyword)))


class TaskModel:

    def __init__(self):
        self.conn = engine.connect()

    def task_result(self):
        task = self.conn.execute(select([amazon_keyword_task.c.id, amazon_keyword_task.c.station,
                                         amazon_keyword_task.c.start_time,
                                         amazon_keyword_task.c.last_update])).fetchall()
        return task


class AmazonTask(TaskModel):

    def show_asin_list(self):
        with engine.connect() as conn:
            results = conn.execute(select([amazon_keyword_task.c.id, amazon_keyword_task.c.station]).where(
                amazon_keyword_task.c.is_add == 0)).fetchall()

            if not results:
                return
            else:
                conn.execute(
                    update(amazon_keyword_task).values({"is_add": 1}).where(amazon_keyword_task.c.is_add == 0))

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

                    result_list = kws_result['result']['list']

                    for i in result_list:
                        conn.execute(update(amazon_keyword_task).values(
                            {"status": i["status"], "monitoring_num": i['monitoring_num'],
                             "monitoring_count": i["monitoring_count"], "monitoring_type": i['monitoring_type'],
                             "start_time": i['start_time'], "end_time": i['end_time'],
                             "created_at": i['created_at'], "deleted_at": i['deleted_at']}).where(
                            amazon_keyword_task.c.id == i['id']))

    # 根据 id  star_time(监控开始时间)  end_time(last_update 最近时间) 调用接口查关键字排名结果写入数据库
    def get_keyword_rank(self):
        with engine.connect() as conn:
            res = self.task_result()
            for i in res:
                get_result = GetAmazonKWMResult(
                    ids=[i[0]],
                    start_time=i[2],
                    end_time=i[3]
                )
                rank_result = get_result.request()
                result_lists = rank_result['result']

                for result_list in result_lists:
                    conver = result_list['keyword_list']
                    asin_list = json.loads(
                        json.dumps(conver).replace('start_time', 'update_time').replace('station', 'site').replace(
                            'keyword_rank', 'rank'))

                    for j in asin_list:
                        for k in sta.keys():
                            if j['site'] == int(k):
                                j['site'] = sta[k]

                    conn.execute(amazon_keyword_rank.insert(), asin_list)

    # 定期维护监控
    def maintain_mon(self):
        # 查找last_update时间字段  判断end_time与last_update超过10天 则判断为不活跃监控，发起del进行删除监控
        with engine.connect() as conn:
            mon_results = conn.execute(select([amazon_keyword_task.c.last_update, amazon_keyword_task.c.end_time,
                                                    amazon_keyword_task.c.id, amazon_keyword_task.c.asin,
                                                    amazon_keyword_task.c.keyword,
                                                    amazon_keyword_task.c.station])).fetchall()

            for mon_result in mon_results:
                last_update = mon_result[0]
                end_time = mon_result[1]
                interval = end_time - last_update

                # 大于10天未活跃 执行del删除该监控
                if interval.days >= 10:
                    del_fail = DelAmazonKWM(
                        ids=[mon_result[2]]
                    )
                    del_fail.request()

                    # is_add 改为0 show_asin_list会重新查结果 并更新为已删除的标识(deleted_at)
                    conn.execute(update(amazon_keyword_task).values({"is_add": 0}).where(
                        amazon_keyword_task.c.id == [mon_result[2]]))

                # end_time 与 last_update 时间间隔不超过1天 判断为活跃监控信息 (当end_time 时间快结束时
                # 调用删除api删除当前监控信息并 添加新的监控api)
                elif interval.days <= 1:
                    del_fail = DelAmazonKWM(
                        ids=[mon_result[2]]
                    )
                    del_fail.request()

                    asin_keywords = [{"asin": mon_result[3], "keyword": mon_result[4]}]
                    cret_amaz_api = AddAmazonKWM(
                        station=mon_result[5],
                        asin_and_keywords=asin_keywords,
                        num_of_days=31,
                        monitoring_num=4
                    )
                    new_result = cret_amaz_api.request()

                    # 重新添加进数据库 id更新
                    result = (new_result['result'])
                    for j in result:
                        insert_stmt = insert(amazon_keyword_task).values(id=j['id'],
                                                                         station=mon_result[5], is_add=0,
                                                                         asin=j['asin'],
                                                                         keyword=j['keyword'],
                                                                         last_update=time_now())
                        on_conflict_stmt = insert_stmt.on_duplicate_key_update({"last_update": time_now()})
                        conn.execute(on_conflict_stmt)


def run():
    input_end = NsqInputEndpoint('haiying.amazon.keyword', 'haiying_crawler', WORKER_NUMBER, **INPUT_NSQ_CONF)
    output_end = NsqOutputEndpoint(**OUTPUT_NSQ_CONF)

    task = AmazonTask()

    server = pipeflow.Server()
    group = server.add_group('main', WORKER_NUMBER)
    group.set_handle(handle, "thread")
    group.add_input_endpoint('input', input_end)
    group.add_output_endpoint('output', output_end, ('test',))

    server.add_routine_worker(task.show_asin_list, interval=60*3)
    server.add_routine_worker(task.get_keyword_rank, interval=60*24*7)
    server.add_routine_worker(task.maintain_mon, interval=60*24*20)

    server.run()
