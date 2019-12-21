from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert

import pipeflow
from pipeflow import NsqInputEndpoint, NsqOutputEndpoint
from config import *
from task_protocol import HYTask
from models.amazon_models import amazon_keyword_task, amazon_keyword_rank
from api.amazon_keyword import GetAmazonKWMAllResult

WORKER_NUMBER = 1
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
        1:'US',
        2:'IT',
        3:'JP',
        4:'GE',
        5:'UK',
        6:'FR',
        7:'ES',
        8:'CA',
        9:'AU',
    }

    _status_map = {
        -2:"InvalidKeyword",
        -1:"InvalidAsin",
        0:"NormalAsin",
        1:"ParentAsin",
        2:"LandingAsin"
    }

    def __init__(self, infos):
        
        self.infos = infos
        self.time_now = datetime.now()
        
    # info = result.get("result", [])
    def parse(self, info):
        parsed_info = {
            "asin": info["asin"],
            "keyword":info["keyword"],
            "status":self._status_map.get(info["status"]),
            "monitoring_num":info["monitoring_num"],
            "monitoring_count":info["monitoring_count"],
            "monitoring_type":info["monitoring_type"],
            "station":self._station_map.get(info["station"]),
            "start_time":info["start_time"],
            "end_time":info["end_time"],
            "created_at":info["created_at"],
            "deleted_at":info["deleted_at"],
            "is_add":info["is_add"],
            "last_update":self.time_now,
        }


        return parsed_info

        # def parsed_infos(self, batch=1000):
        #     info_cnt = len(self.infos)
        #     i = 0
        #     while i < info_cnt:
        #         yield list(map(self.parse, self.infos[i:i + batch]))
        #         i += batch
        
        
class HYKeyWordTask(HYTask):
    
    @property
    def station(self):
        return self.task_data['station']


def handle(group, task):
    hy_task = HYKeyWordTask(task)
    result = GetAmazonKWMAllResult('US').request()
    print(result)







# if __name__ == '__main__':
def run():
    input_end = NsqInputEndpoint('haiying.amazon.keyword', 'haiying_crawler', 2, **INPUT_NSQ_CONF)

    server = pipeflow.Server()
    group = server.add_group('main', 2)
    group.set_handle(handle, "thread")
    group.add_input_endpoint('input', input_end)

    server.run()
