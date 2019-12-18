import re
import json
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.sql import select, and_, bindparam
import pipeflow
from pipeflow import NsqInputEndpoint, NsqOutputEndpoint
from task_protocol import HYTask
from config import *
from api.amazon_category import GetAmazonCategory
from models.amazon_models import amazon_category
from util.pub import pub_to_nsq
from util.log import logger

WORKER_NUMBER = 1
TOPIC_NAME = 'haiying.amazon.category'

engine = create_engine(
    SQLALCHEMY_DATABASE_URI,
    pool_pre_ping=SQLALCHEMY_POOL_PRE_PING,
    echo=SQLALCHEMY_ECHO,
    pool_size=SQLALCHEMY_POOL_SIZE,
    max_overflow=SQLALCHEMY_POOL_MAX_OVERFLOW,
    pool_recycle=SQLALCHEMY_POOL_RECYCLE,
)


class CategoryInfo:

    def __init__(self, site, infos):
        self.site = site
        self.infos = infos
        self.time_now = datetime.now()

    def parse(self, info):
        parsed_info = {
            "category_id": info["cate_id"],
            "category_name": info["cate_name"],
            "parent_id": "",
            "level": info["level"],
            "is_leaf": info["is_leaf"],
            "site": self.site,
            "category_id_path": "",
            "category_name_path": "",
            "hy_create_time": None,
            "update_time": self.time_now,
        }
        i = info["created_date"].find(".")
        if i != -1:
            info["created_date"] = info["created_date"][:i]
        parsed_info["hy_create_time"] = datetime.strptime(info["created_date"],
                                                          "%Y-%m-%d %H:%M:%S")
        level_id_ls = []
        level_name_ls = []
        for k in info:
            if k.startswith("p_l") and info[k]:
                reg = re.search(r"p_l(?P<level>\d+)_(?P<type>\w+)", k)
                if reg:
                    tmp = reg.groupdict()
                    if tmp['type'] == 'id':
                        level_id_ls.append({"level": int(tmp["level"]), "value": info[k]})
                    elif tmp['type'] == 'name':
                        level_name_ls.append({"level": int(tmp["level"]), "value": info[k]})
        level_id_ls.sort(key=lambda x:x["level"])
        level_name_ls.sort(key=lambda x:x["level"])
        level_id_ls.append({"level": info["level"], "value": info["cate_id"]})
        level_name_ls.append({"level": info["level"], "value": info["cate_name"]})
        parsed_info["category_id_path"] = ":".join(item["value"] for item in level_id_ls)
        parsed_info["category_name_path"] = ":".join(item["value"] for item in level_name_ls)
        parsed_info["parent_id"] = level_id_ls[-1]["value"]
        return parsed_info

    def parsed_infos(self, batch=500):
        info_cnt = len(self.infos)
        i = 0
        while i < info_cnt:
            yield list(map(self.parse, self.infos[i:i+batch]))
            i += batch


def handle(group, task):
    hy_task = HYTask(task)
    site = hy_task.task_data['site']
    station = site.upper()
    result = GetAmazonCategory(station).request()
    if result["status"] == "success":
        categorys = CategoryInfo(site, result["result"])
        with engine.connect() as conn:
            for infos in categorys.parsed_infos():
                category_id_paths = map(lambda x:x["category_id_path"], infos)
                old_records = conn.execute(
                    select([amazon_category.c.category_id_path, amazon_category.c.hy_create_time])
                    .where(
                        and_(
                            amazon_category.c.category_id_path.in_(category_id_paths),
                            amazon_category.c.site == site,
                        )
                    )).fetchall()
                old_records_map = {item[amazon_category.c.category_id_path]:
                                item[amazon_category.c.hy_create_time]
                                for item in old_records}
                update_records = []
                add_records = []
                for info in infos:
                    if info['category_id_path'] not in old_records_map:
                        add_records.append(info)
                    elif info['hy_create_time'] > old_records_map[info['category_id_path']]:
                        update_records.append(info)
                if add_records:
                    conn.execute(amazon_category.insert(), add_records)
                if update_records:
                    for item in update_records:
                        item['_id'] = item['category_id_path']
                        item['_site'] = item['site']
                    conn.execute(
                        amazon_category.update()
                        .where(
                            and_(
                               amazon_category.c.category_id_path == bindparam('_id'),
                               amazon_category.c.site == bindparam('_site')
                            )
                        )
                        .values(
                            category_id=bindparam('category_id'),
                            category_name=bindparam('category_name'),
                            parent_id=bindparam('parent_id'),
                            level=bindparam('level'),
                            is_leaf=bindparam('is_leaf'),
                            category_name_path=bindparam('category_name_path'),
                            hy_create_time=bindparam('hy_create_time'),
                            update_time=bindparam('update_time')
                        ),
                        update_records
                    )


async def create_task():
    sites = ['us']
    for site in sites:
        task = {
            "task": "amazon_category_sync",
            "data": {
                "site": site,
            }
        }
        await pub_to_nsq(NSQ_NSQD_HTTP_ADDR, TOPIC_NAME, json.dumps(task))


def run():
    input_end = NsqInputEndpoint(TOPIC_NAME, 'haiying_crawler', WORKER_NUMBER,  **INPUT_NSQ_CONF)

    server = pipeflow.Server()
    group = server.add_group('main', WORKER_NUMBER)
    group.set_handle(handle, "thread")
    group.add_input_endpoint('input', input_end)

    server.add_routine_worker(create_task, interval=60*24*7, immediately=True)
    server.run()
