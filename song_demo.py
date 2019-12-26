# import asyncio
import json
# import time
# from datetime import datetime, timedelta

import nsq
import tornado.ioloop
# from sqlalchemy import create_engine
#
# from HY_keyword import TOPIC_NAME
# from api.amazon_keyword import GetAmazonKWMStatus, AddAmazonKWM, GetAmazonKWMAllResult, GetAmazonKWMResult
# from models.amazon_models import amazon_keyword_task
# from task_protocol import HYTask
# from sqlalchemy.sql import  select
# # from config import *
# from util.pub import pub_to_nsq
#
# engine = create_engine(
#     # 链接地址
#     SQLALCHEMY_DATABASE_URI,
#     # 连接池
#     pool_pre_ping=SQLALCHEMY_POOL_PRE_PING,
#     # 日志等级
#     echo=SQLALCHEMY_ECHO,
#     # 连接池大小
#     pool_size=SQLALCHEMY_POOL_SIZE,
#     # 连接池溢出,仅限于queuepool
#     max_overflow=SQLALCHEMY_POOL_MAX_OVERFLOW,
#     # 池回收时间
#     pool_recycle=SQLALCHEMY_POOL_RECYCLE,
# )
#
# def getstatus():
#     with engine.connect() as conn:
#         result_from_db = select(([amazon_keyword_task]))
#         conn.execute()
#     result = GetAmazonKWMStatus(
#         ids=[],
#         station='JP',
#         capture_status=0,
#     ).request()
#     print(result,"getstatus")
#
#
# def addmonkwasins():
#     result = AddAmazonKWM(
#         station = 'US',
#         asin_and_keywords =[{"asin":"B07PY52GVP","keyword":"XiaoMi"}],
#         num_of_days = 31,
#         monitoring_num = 24,
#     ).request()
#     print(result,"add_a_kw_watch")
#
# def getallresult():
#     with engine.connect() as conn:
#         idss = conn.execute(select([amazon_keyword_task.c.id])).fetchall()
#     print(idss)
#     ids = [x for x in idss]
#     result = GetAmazonKWMStatus(
#         station='US',
#         capture_status = '6',
#         ids = ids
#
#     ).request()
#     print(result)
#
# def getallkwasin():
#     result = GetAmazonKWMAllResult(
#         station='US',
#     ).request()
#     print(result)
#
# class add_crawler():
#     def __init__(self, ):
#         pass
#
# def getkwrank():
#     result =  GetAmazonKWMResult(
#         # ids = [x for x in range(234615,234630)],
#         ids = [235652,235651],
#         start_time= '2019-12-12 12:00:00',
#         end_time=  '2020-01-26 14:00:00',
#         # start_time='1',
#         # end_time='1',
#     ).request()
#     print(result)
#
#
#
#
# def map_test(info):
#     parsed_info = {
#                 "id": info["id"],
#                 "asin": info["asin"],
#                 "keyword": info["keyword"],
#                 "status": info["status"],
#                 "monitoring_num": info["monitoring_num"],
#                 "monitoring_count": info["monitoring_count"],
#                 "monitoring_type": info["monitoring_type"],
#                 "station": info["station"],
#                 "start_time": info["start_time"],
#                 "end_time": info["end_time"],
#                 "created_at": info["created_at"],
#                 "deleted_at": info["deleted_at"],
#                 "is_add": 1,
#                 "last_update": datetime.now(),
#             }
#     return list(map(parsed_info, info))


def pub_message():
    message = {"task": "haiying.amazon.keyword", "data": {"site": "JP", "asin": "B072M34RQC", "keyword": "Monitor&qid"}}
    writer.pub('haiying.amazon.keyword',json.dumps(message))

def finish_pub(conn, data):
    print(data)

if __name__ == '__main__':
    writer = nsq.Writer(['127.0.0.1:4150'])
    tornado.ioloop.PeriodicCallback(pub_message, 2000).start()
    nsq.run()
    # start_time = datetime.now()
    # str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    # # getkwrank()
    # # getstatus()
    # end_time = time.strftime('%Y-%m-%d %H:%M:%S')
    # end_time_2 = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
    # print(start_time,"\n",end_time, "\n", end_time_2)
    # a= datetime.now() - timedelta(days=1)
    # print(a)
    # getallresult()
    # getallkwasin()'%Y-%m-%d %H:%M:%S'
    # info = {'id': 235664, 'asin': 'B002JOO448', 'keyword': 'football', 'status': 'NormalAsin', 'monitoring_num': 4, 'monitoring_count': 0, 'monitoring_type': 3, 'station': 'US', 'start_time': '2019-12-26 12:12:55', 'end_time': '2020-01-25 12:01:55', 'created_at': '2019-12-26 12:12:17', 'deleted_at': None, 'is_add': 1, 'last_update': '2019-12-26 13:23:30'}
    # print(map_test(info))