import json
from datetime import datetime

from sqlalchemy import create_engine, select, and_
from sqlalchemy.dialects.mysql import insert

import pipeflow
from pipeflow import NsqInputEndpoint, NsqOutputEndpoint
from config import *
from task_protocol import HYTask
from models.amazon_models import amazon_keyword_task, amazon_keyword_rank
from api.amazon_keyword import GetAmazonKWMAllResult, GetAmazonKWMStatus, AddAmazonKWM, GetAmazonKWMResult
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

    def parsed_infos(self, batch=1000):
        info_cnt = len(self.infos)
        i = 0
        while i < info_cnt:
            yield list(map(self.parse, self.infos[i:i + batch]))
            i += batch

class KeywordRankInfo:

    def __init__(self, infos):
        self.infos = infos
        self.time_now = datetime.now()

    def parse(self, info):
        parsed_info = {
            "asin": info["asin"],
            "keyword": info["keyword"],
            "site": info['station'],
            "rank": info["keyword_rank"],
            "aid": info["aid"],
            "update_time": self.time_now,
        }
        return parsed_info

    def parsed_infos(self, batch=1000):
        info_cnt = len(self.infos)
        i = 0
        while i < info_cnt:
            yield list(map(self.parse, self.infos[i:i + batch]))
            i += batch


class HYKeyWordTask(HYTask):

    @property
    def params(self):
        dct = {}
        if self.task_data.get('station'):
            dct['station'] = self.task_data['station']
        if self.task_data.get('asin_and_keywords'):
            dct['asin_and_keywords'] = self.task_data['asin_and_keywords']
        if self.task_data.get('num_of_days'):
            dct['num_of_days'] = self.task_data['num_of_days']
        if self.task_data.get('monitoring_num'):
            dct['monitoring_num'] = self.task_data['monitoring_num']
        return dct






def handle(group, task):
    print('a')
    # 获取NSQ中的任务参数
    hy_task = HYKeyWordTask(task)
    print(hy_task.data)
    params = hy_task.params
    station = params.get('station')
    for item in params.get('asin_and_keywords'):
        asin = item['asin']
        keyword = item['keyword']
    # 根据任务参数决定任务性质即
    # 获取商品ID,关键词,站点HYKeyWordTask.params
        with engine.connect() as conn:
            result_from_db = conn.execute(
                select([amazon_keyword_rank])
                .where(
                    and_(
                        amazon_keyword_rank.c.asin == asin,
                        amazon_keyword_rank.c.keyword == keyword,
                        amazon_keyword_rank.c.site == station.upper()
                    )
                )
            ).fetchall()
            # 数据库存在kw_rank
            if result_from_db:
                result_from_db_kwrank = {item[amazon_keyword_rank.c.keyword]:
                                         item[amazon_keyword_rank.c.rank]
                                         for item in result_from_db}
                print(result_from_db_kwrank)
            # 数据库不存在kw_rank
            else:
                result_from_HY = GetAmazonKWMResult(
                    ids = params.get('ids'),
                    start_time = '',
                    end_time = '',
                ).request()
                if result_from_HY['msg'] == 'success':
                    keyword_list = KeywordRankInfo(result_from_HY)
                    print(keyword_list)

    # 查询数据库amazon_keyword_rank,
    # if 存在,按参数取出数据返回
    # else不存在则向HY下发爬取任务,带参数商品ID,关键词,站点,查询频率

async def create_hy_task():
    stations = ['US']
    for station in stations:
        task = {
            "task": "amazon_keyword_sync",
            "data": {
                "station": station,
                "asin_and_keywords": '',
                "num_of_days": 30,
                "monitoring_num": 48,
            }
        }
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