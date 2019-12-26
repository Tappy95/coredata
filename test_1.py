import json
from datetime import datetime, timedelta

from sqlalchemy import create_engine

from api.amazon_keyword import GetAmazonKWMStatus, AddAmazonKWM, GetAmazonKWMAllResult
from models.amazon_models import amazon_keyword_task
from task_protocol import HYTask
from sqlalchemy.sql import  select
from config import *
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

def getstatus():
    result = GetAmazonKWMStatus(
        ids=[1],
        station='jp',
        capture_status=0,
    ).request()
    print(result,"getstatus")


def addmonkwasins():
    result = AddAmazonKWM(
        station = 'jp',
        asin_and_keywords ='',
        num_of_days = 30,
        monitoring_num = 48,
    ).request()
    print(result,"add_a_kw_watch")

def getallresult():
    # with engine.connect() as conn:
    #     idss = conn.execute(select(amazon_keyword_task.c.id)).fetchall()
    #     print(idss)

    result = GetAmazonKWMStatus(
        station='US',
        capture_status = '2',
        ids = [234599]

    ).request()
    print(result)

def time_test():
    time_1 = datetime.now() - timedelta(days=30)

    print(time_1)

if __name__ == '__main__':

    # addmonkwasins()
    # getallresult()

    time_test()
    print("us".upper())

# curl -d '{"task": "haiying.amazon.keyword", "data": {"site": "JP", "asin": "B072M34RQC", "keyword": "Monitor&qid"}}' "http://127.0.0.1:4151/pub?topic=haiying.amazon.keyword"