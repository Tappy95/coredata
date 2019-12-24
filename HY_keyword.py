import asyncio

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


# 环境变量,worker数量
from util.pub import pub_to_nsq

WORKER_NUMBER = 2
TOPIC_NAME = 'haiying.amazon.category'

# 创建数据库引擎
engine = create_engine(
    # 链接地址
    SQLALCHEMY_DATABASE_URI,
    # 连接池
    pool_pre_ping=SQLALCHEMY_POOL_PRE_PING,
    # 日志等级
    echo=SQLALCHEMY_ECHO,
    # 连接池大小
    pool_size=SQLALCHEMY_POOL_SIZE,
    # 连接池溢出,仅限于queuepool
    max_overflow=SQLALCHEMY_POOL_MAX_OVERFLOW,
    # 池回收时间
    pool_recycle=SQLALCHEMY_POOL_RECYCLE,
)

# 国家字典
sta = {1: 'US', 2: 'IT', 3: 'JP', 4: 'DE', 5: 'UK', 6: 'FR', 7: 'ES', 8: 'CA', 9: 'AU'}


# 返回当前时间
def time_now():
    # strftime解析时间("年月日时分秒")
    now_time = time.strftime("%Y-%m-%d %H:%M:%S")
    return now_time

class HYKeywordTask(HYTask):
    @classmethod
    @property
    def params(cls):
        pass


def handle(group, task):
    hy_task = HYTask(task)
    station = hy_task.s

def create_task():
    stations = ['US']
    for site in stations:
        task = {
            "task": "amazon_category_sync",
            "data": {
                "site": site,
            }
        }
        pub_to_nsq(NSQ_NSQD_HTTP_ADDR, TOPIC_NAME, json.dumps(task))

def run():

    # 创建NSQ输入点,参数为
    # """NSQ input endpoint
    #
    #     input_end = NsqInputEndpoint('topic_x', 'channel_x', 3,  **{'lookupd_http_addresses': ['127.0.0.1:5761']})
    #                                   主题    ,   分类      , 最大worker数,  不定长键值对:nsq节点地址:端口
    #     """
    input_end = NsqInputEndpoint('haiying.amazon.keyword', 'haiying_crawler', WORKER_NUMBER, **INPUT_NSQ_CONF)
    # 创建输出点,参数为
    # """NSQ output endpoint
    #
    #     output_end = NsqOutputEndpoint(**{'nsqd_tcp_addresses': '127.0.0.1:5750'})
    #                                               nsqd接收,排队将消息传递到指定地址
    #  """
    #
    output_end = NsqOutputEndpoint(**OUTPUT_NSQ_CONF)

    # 创建AmazonTask对象,调用海鹰的API接口获取用户监控的商品信息

    # 创建pipeflow对象
    server = pipeflow.Server()
    # 创建pipeflow对象中的组对象(参数包括name, concurrency=WORKER_NUMBER, task_queue_size=None[任务队列大小])
    group = server.add_group('main', WORKER_NUMBER)
    # 设置每个组的任务
    # "thread"以线程方式工作
    group.set_handle(handle, "thread")
    # 在组的基础上加入输入点
    group.add_input_endpoint('input', input_end)
    # 在组的基础上加入输出点,('test',)输出名字典参数
    group.add_output_endpoint('output', output_end, ('test',))
    # pipeflow加入常规worker(任务,间隔秒(两种模式,定时crontab和间隔interval),立刻开始)
    # 获取商品ID列表-asin_list
    # server.add_routine_worker(task.show_asin_list, interval=60*3, immediately=True)
    # # 获取关键词rank排名,以间隔方式运行
    # server.add_routine_worker(task.get_keyword_rank, interval=60*24*7)
    # # 保持监控正常更新,查DB中的end_time和update_time
    # server.add_routine_worker(task.maintain_mon, interval=60*24*20)

    server.run()

if __name__ == '__main__':
    create_task()
