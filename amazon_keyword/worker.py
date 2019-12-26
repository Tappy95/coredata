import json
from datetime import datetime, timedelta

from sqlalchemy import create_engine, select, and_
from sqlalchemy.dialects.mysql import insert

import pipeflow
from pipeflow import NsqInputEndpoint, NsqOutputEndpoint
from config import *
from task_protocol import HYTask
from models.amazon_models import amazon_keyword_task, amazon_keyword_rank
from api.amazon_keyword import GetAmazonKWMStatus, AddAmazonKWM, GetAmazonKWMResult
from util.pub import pub_to_nsq

WORKER_NUMBER = 2
TOPIC_NAME = 'haiying.amazon.keyword'

engine = create_engine(
    SQLALCHEMY_DATABASE_URI,
    pool_pre_ping=SQLALCHEMY_POOL_PRE_PING,
    echo=SQLALCHEMY_ECHO,
    pool_size=SQLALCHEMY_POOL_SIZE,
    max_overflow=SQLALCHEMY_POOL_MAX_OVERFLOW,
    pool_recycle=SQLALCHEMY_POOL_RECYCLE,
)


class KeywordTaskInfo:
    station_map = {
        1: 'US',
        2: 'IT',
        3: 'JP',
        4: 'GE',
        5: 'UK',
        6: 'FR',
        7: 'ES',
        8: 'CA',
        9: 'AU',
    }

    def __init__(self):
        self.time_now = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')

    def parse(self, info):
        parsed_info = {
            "id": info["id"],
            "asin": info["asin"],
            "keyword": info["keyword"],
            "status": info["status"],
            "monitoring_num": info["monitoring_num"],
            "monitoring_count": info["monitoring_count"],
            "monitoring_type": info["monitoring_type"],
            "station": self.station_map.get(info["station"]),
            "start_time": info["start_time"],
            "end_time": info["end_time"],
            "created_at": info["created_at"],
            "deleted_at": info["deleted_at"],
            "is_add": 1,
            "last_update": self.time_now,
        }
        return parsed_info


class KeywordRankInfo:

    def __init__(self, infos):
        self.infos = infos
        self.time_now = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')

    # TODO: API不可用,这里写死了测试
    def parse(self, info):
        parsed_info = {
            "asin": info["asin"],
            "keyword": info["keyword"],
            # "site": KeywordTaskInfo.station_map.get(info['station']),
            "site": 'US',
            # "rank": info["keyword_rank"],
            "rank": 1,
            # "aid": info["aid"],
            "aid": 1,
            "update_time": self.time_now,
        }
        return parsed_info

    def parsed_infos(self, batch=1000):
        info_cnt = len(self.infos)
        i = 0
        while i < info_cnt:
            yield list(map(self.parse, self.infos[i:i + batch]))
            i += batch

# TODO:写完了,没优化...
def handle(group, task):
    # 获取NSQ中的任务参数(虚拟爬虫已经检查过rankdb中没有数据)
    hy_task = HYTask(task)
    site = hy_task.task_data['site']
    asin = hy_task.task_data['asin']
    keyword = hy_task.task_data['keyword']
    # 用任务参数查DB中的task表
    with engine.connect() as conn:
        select_task = select([amazon_keyword_task]) \
            .where(
            and_(
                amazon_keyword_task.c.station == site.upper(),
                amazon_keyword_task.c.asin == asin,
                amazon_keyword_task.c.keyword == keyword
            )
        )
        select_task_info = conn.execute(select_task).first()
        # 若task中存在此关键词监控任务
        print("是否有关键词监控任务", select_task_info)
        if select_task_info != None:
            print("task表中有任务,根据参数查排名纪录")
            ids = [select_task_info['id']]
            # 默认查询当天的关键词排名记录
            start_time = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
            end_time = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
            # 从数据库中获取参数

            # 向海鹰下发获取关键词排名监控记录
            result = GetAmazonKWMResult(
                ids=ids,
                start_time=start_time,
                end_time=end_time,
            ).request()
            # 获取成功,解析json后持久化到DB
            if result and result['msg'] == 'success':
                print("获取关键词排名", result['result'])
                keyword_rank = KeywordRankInfo(result.get("result", []))
                for infos in keyword_rank.parsed_infos():
                    insert_stmt = insert(amazon_keyword_rank)
                    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
                        asin=insert_stmt.inserted.asin,
                        keyword=insert_stmt.inserted.keyword,
                        site=insert_stmt.inserted.site,
                        rank=insert_stmt.inserted.rank,
                        aid=insert_stmt.inserted.aid,
                        update_time=insert_stmt.inserted.update_time,
                    )
                    conn.execute(on_duplicate_key_stmt, infos)
                    print("更新amazon_rank成功")
        # task表中无此任务,发布新的监控排名任务
        # TODO:有bug,没找出来
        else:
            print("没有关键词任务,创建任务")
            asin_and_keywords = [{"asin": asin, "keyword": keyword}]
            num_of_days = 30
            monitoring_num = 4
            result_from_addtask = AddAmazonKWM(
                station=site,
                asin_and_keywords=asin_and_keywords,
                num_of_days=num_of_days,
                monitoring_num=monitoring_num,
            ).request()
            print("在HY创建监控任务", result_from_addtask)
            # 获取添加任务的ID,向海鹰获取监控商品信息
            if result_from_addtask and result_from_addtask["msg"] == "success":
                task_id = []
                for one_task in result_from_addtask['result']:
                    task_id.append(one_task['id'])
                asin_list_result = GetAmazonKWMStatus(
                    station=site,
                    capture_status=2,
                    ids=task_id,
                ).request()
                print("获取 任务ID", asin_list_result)
                if asin_list_result['msg'] == "success":
                    for total_task in asin_list_result['result']['list']:
                        keyword_taskinfo = KeywordTaskInfo()
                        print(total_task)
                        with engine.connect() as conn:
                            print(keyword_taskinfo.parse(total_task))
                            infos = keyword_taskinfo.parse(total_task)
                            insert_stmt = insert(amazon_keyword_task)
                            onduplicate_key_stmt = insert_stmt.on_duplicate_key_update(
                                id=insert_stmt.inserted.id,
                                asin=insert_stmt.inserted.asin,
                                keyword=insert_stmt.inserted.keyword,
                                status=insert_stmt.inserted.status,
                                monitoring_num=insert_stmt.inserted.monitoring_num,
                                monitoring_count=insert_stmt.inserted.monitoring_count,
                                monitoring_type=insert_stmt.inserted.monitoring_type,
                                station=insert_stmt.inserted.station,
                                start_time=insert_stmt.inserted.start_time,
                                end_time=insert_stmt.inserted.end_time,
                                created_at=insert_stmt.inserted.created_at,
                                deleted_at=insert_stmt.inserted.deleted_at,
                                is_add=insert_stmt.inserted.start_time,
                                last_update=insert_stmt.inserted.last_update,
                            )
                            conn.execute(onduplicate_key_stmt, infos)
                    print("更新amazon_task成功")
                    # 通过参数发任务更新rank
                    ids = [asin_list_result['result']['list'][0]['id']]
                    # 默认查询当天的关键词排名记录
                    start_time = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
                    end_time = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
                    # 从数据库中获取参数
                    # 向海鹰下发获取关键词排名监控记录
                    result = GetAmazonKWMResult(
                        ids=ids,
                        start_time=start_time,
                        end_time=end_time,
                    ).request()
                    # 获取成功,解析json后持久化到DB
                    if result and result['msg'] == 'success':
                        print("获取关键词排名", result['result'])
                        keyword_rank = KeywordRankInfo(result.get("result", []))
                        for infos in keyword_rank.parsed_infos():
                            insert_stmt = insert(amazon_keyword_rank)
                            on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
                                asin=insert_stmt.inserted.asin,
                                keyword=insert_stmt.inserted.keyword,
                                site=insert_stmt.inserted.site,
                                rank=insert_stmt.inserted.rank,
                                aid=insert_stmt.inserted.aid,
                                update_time=insert_stmt.inserted.update_time,
                            )
                            conn.execute(on_duplicate_key_stmt, infos)
                            print("更新amazon_rank成功")


async def create_hy_task():
    sites = ['US']
    for site in sites:
        task = {
            "task": "haiying.amazon.keyword",
            "data": {
                "site": site,
                "asin": "B07KMM96GV",
                "keyword": "airjordans 11",
            }
        }
        print(json.dumps(task))
        await pub_to_nsq(NSQ_NSQD_HTTP_ADDR, TOPIC_NAME, json.dumps(task))


def run():
    input_end = NsqInputEndpoint(TOPIC_NAME, 'haiying_crawler', WORKER_NUMBER, **INPUT_NSQ_CONF)
    output_end = NsqOutputEndpoint(**OUTPUT_NSQ_CONF)

    server = pipeflow.Server()
    group = server.add_group('main', WORKER_NUMBER)
    group.set_handle(handle, "thread")
    group.add_input_endpoint('input', input_end)
    group.add_output_endpoint('output', output_end)

    server.add_routine_worker(create_hy_task, interval=60 * 24 * 7, immediately=True)
    print("run server")
    server.run()
    print("end")

if __name__ == '__main__':
    run()
