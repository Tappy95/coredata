import json
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert

import pipeflow
from pipeflow import NsqInputEndpoint, NsqOutputEndpoint
from config import *
from task_protocol import HYTask
from models.amazon_models import amazon_keyword_task, amazon_keyword_rank
from api.amazon_keyword import GetAmazonKWMAllResult, GetAmazonKWMStatus, AddAmazonKWM
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
    _station_map = {
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

    _status_map = {
        -2: "InvalidKeyword",
        -1: "InvalidAsin",
        0: "NormalAsin",
        1: "ParentAsin",
        2: "LandingAsin"
    }

    def __init__(self, infos):
        self.infos = infos
        self.time_now = datetime.now()

    # info = result.get("result", [])
    def parse(self, info):
        parsed_info = {
            "asin": info["asin"],
            "keyword": info["keyword"],
            "status": self._status_map.get(info["status"]),
            "monitoring_num": info["monitoring_num"],
            "monitoring_count": info["monitoring_count"],
            "monitoring_type": info["monitoring_type"],
            "station": self._station_map.get(info["station"]),
            "start_time": info["start_time"],
            "end_time": info["end_time"],
            "created_at": info["created_at"],
            "deleted_at": info["deleted_at"],
            "is_add": info["is_add"],
            "last_update": self.time_now,
        }

        return parsed_info


class HYKeyWordTask(HYTask):

    @property
    def params(self):
        dct = {}
        if self.task_data.get('station'):
            dct['station'] = self.task_data['station']
        # if self.task_data.get('asin_and_keywords'):
        #     dct['asin_and_keywords'] = self.task_data['asin_and_keywords']
        if self.task_data.get('num_of_days'):
            dct['num_of_days'] = self.task_data['num_of_days']
        if self.task_data.get('monitoring_num'):
            dct['monitoring_num'] = self.task_data['monitoring_num']
        dct['asin_and_keywords'] = [{"asin": "B07PY52GVP", "keyword": "XiaoMi"}]
        return dct


def handle(group, task):
    print('a')
    hy_task = HYKeyWordTask(task)
    params = hy_task.params
    result = GetAmazonKWMStatus(**params).request()
    print(hy_task.data)
    print(result)
    # 获取查询参数
    if result["status"] == "success":
        Kw_Task = KeywordTaskInfo(result["result"])
        with engine.connect() as conn:
            for infos in amazon_keyword_task.parsed_infos():
                insert_stmt = insert(amazon_keyword_task)
                on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
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
                    is_add=insert_stmt.inserted.is_add,
                    last_update=insert_stmt.inserted.last_update,
                )
                conn.execute(on_duplicate_key_stmt, infos)
    # 有返回数据,没有下发任务


async def create_task():
    stations = ['US']
    for station in stations:
        task = {
            "task": "amazon_keyword_sync",
            "data": {
                "station": station,
                "asin_and_keywords": "",
                "num_of_days": 30,
                "monitoring_num": 48,
            }
        }
        await pub_to_nsq(NSQ_NSQD_HTTP_ADDR, TOPIC_NAME, json.dumps(task))


def run():
    input_end = NsqInputEndpoint(TOPIC_NAME, 'haiying_crawler', WORKER_NUMBER, **INPUT_NSQ_CONF)
    print(1)
    output_end = NsqOutputEndpoint(**OUTPUT_NSQ_CONF)
    print(1)

    server = pipeflow.Server()
    print(1)
    group = server.add_group('main', WORKER_NUMBER)
    print(1)
    group.set_handle(handle, "thread")
    print(1)
    group.add_input_endpoint('input', input_end)
    print(1)
    group.add_output_endpoint('output', output_end)
    print(1)

    server.add_routine_worker(create_task, interval=60 * 24 * 7, immediately=True)
    print(8)
    server.run()
    print(9)


if __name__ == '__main__':
    run()
