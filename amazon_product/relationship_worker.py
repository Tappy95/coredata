import re
import json
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.sql import select, and_, bindparam
import pipeflow
from pipeflow import NsqInputEndpoint, NsqOutputEndpoint
from task_protocol import HYTask
from config import *
from models.amazon_models import amazon_category, amazon_product_relationship
from util.pub import mpub_to_nsq
from util.log import logger


TOPIC_NAME = 'haiying.amazon.product'

engine = create_engine(
    SQLALCHEMY_DATABASE_URI,
    pool_pre_ping=SQLALCHEMY_POOL_PRE_PING,
    echo=SQLALCHEMY_ECHO,
    pool_size=SQLALCHEMY_POOL_SIZE,
    max_overflow=SQLALCHEMY_POOL_MAX_OVERFLOW,
    pool_recycle=SQLALCHEMY_POOL_RECYCLE,
)


async def clean_relationship():
    time_to_delete = datetime.now() - timedelta(days=30)
    with engine.connect() as conn:
        conn.execute(
            amazon_product_relationship.delete()
            .where(amazon_product_relationship.c.update_time < time_to_delete)
        )


async def create_task():
    sites = ['us']
    limit = 500
    for site in sites:
        offset = 0
        category_id_set = set([])
        category_id_path_ls = []
        with engine.connect() as conn:
            while True:
                records = conn.execute(
                    select([amazon_category.c.category_id,
                            amazon_category.c.category_id_path])
                    .where(amazon_category.c.site == site)
                    .limit(limit).offset(offset)
                ).fetchall()
                for record in records:
                    if record[amazon_category.c.category_id] not in category_id_set:
                        category_id_path_ls.append(record[amazon_category.c.category_id_path])
                        category_id_set.add(record[amazon_category.c.category_id])
                if len(records) < limit:
                    break
                offset += len(records)
        random.shuffle(category_id_path_ls)
        i = 0
        pub_limit = 200
        while i < len(category_id_path_ls):
            task_ls = [json.dumps({
                "task": "amazon_product_sync",
                "data": {
                    "site": site,
                    "category_id_path": category_id_path
                }
            }) for category_id_path in category_id_path_ls[i:i+pub_limit]]
            await mpub_to_nsq(NSQ_NSQD_HTTP_ADDR, TOPIC_NAME, task_ls)
            i += pub_limit


def run():
    server = pipeflow.Server()

    #server.add_routine_worker(clean_relationship, interval=5)
    server.add_routine_worker(create_task, interval=60*24*7, immediately=True)
    server.run()
