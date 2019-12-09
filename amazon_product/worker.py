import re
import json
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.sql import select, and_, bindparam
import pipeflow
from pipeflow import NsqInputEndpoint, NsqOutputEndpoint
from task_protocol import HYTask
from config import *
from api.amazon_product import GetAmazonProductBySearch
from models.amazon_models import amazon_product
from util.pub import pub_to_nsq
from util.log import logger

WORKER_NUMBER = 10
TOPIC_NAME = 'haiying.amazon.product'

engine = create_engine(
    SQLALCHEMY_DATABASE_URI,
    pool_pre_ping=SQLALCHEMY_POOL_PRE_PING,
    echo=SQLALCHEMY_ECHO,
    pool_size=SQLALCHEMY_POOL_SIZE,
    max_overflow=SQLALCHEMY_POOL_MAX_OVERFLOW,
    pool_recycle=SQLALCHEMY_POOL_RECYCLE,
)


class productInfo:

    _deliver_map = {
        "Amazon": 0,
        "FBM": 1,
        "FBA": 2,
        "other": 3
    }

    def __init__(self, infos):
        self.infos = infos
        self.time_now = datetime.now()

    def parse_timestr(self, timestr, ptype='last_upd_date'):
        if ptype == 'last_upd_date':
            time_format_str = "%Y-%m-%d %H:%M:%S"
        else:
            time_format_str = "%Y-%m-%d"
        i = timestr.find(".")
        if i != -1:
            return datetime.strptime(timestr[:i], time_format_str)
        else:
            return datetime.strptime(timestr, time_format_str)

    def parse(self, info):
        parsed_info = {
            "asin": info["asin"],
            "parent_asin": info["parent_asin"],
            "merchant_id": info["merchant_code"],
            "merchant_name": info["merchant"],
            "delivery": self._deliver_map.get(info["delivery"], 3),
            "reviews_number": info["customer_reviews"],
            "review_score": info["score"],
            "seller_number": info["follow_sellers_num"],
            "qa_number": info["answered_questions"],
            "not_exist": info["not_exist"],
            "status": info["status"],
            "price": info["asin_price_min"],
            "shipping_weight": info["shipping_weight"],
            "img": info["img_url"],
            "title": info["title"],
            "brand": info["brand"],
            "is_amazon_choice": info["is_ama_choice"],
            "is_best_seller": info["is_best_seller"],
            "is_prime": info["is_prime"],
            "first_arrival": self.parse_timestr(info["fir_arrival"], "fir_arrival") \
                            if info["fir_arrival"] else None,
            "hy_update_time": self.parse_timestr(info["last_upd_date"]),
            "update_time": self.time_now,
            "imgs": ",".join(info["asin_images_url"]),
            "description": info["prod_desc"],
        }
        return parsed_info

    def parsed_infos(self, batch=100):
        info_cnt = len(self.infos)
        i = 0
        while i < info_cnt:
            yield list(map(self.parse, self.infos[i:i+batch]))
            i += batch


def handle(group, task):
    hy_task = HYTask(task)
    station = hy_task.task_data['site'].upper()
    asins = hy_task.task_data['asins']
    result = GetAmazonProductBySearch(station, asins=asins).request()
    if result["status"] == "success":
        products = productInfo(result["result"])
        with engine.connect() as conn:
            for infos in products.parsed_infos():
                asins = map(lambda x:x["asin"], infos)
                old_records = conn.execute(
                    select([amazon_product.c.asin, amazon_product.c.hy_update_time])
                    .where(
                        amazon_product.c.asin.in_(asins)
                    )).fetchall()
                old_records_map = {item[amazon_product.c.asin]:
                                item[amazon_product.c.hy_update_time]
                                for item in old_records}
                update_records = []
                add_records = []
                for info in infos:
                    if info['asin'] not in old_records_map:
                        add_records.append(info)
                    elif info['hy_update_time'] > old_records_map[info['asin']]:
                        update_records.append(info)
                if add_records:
                    conn.execute(amazon_product.insert(), add_records)
                if update_records:
                    for item in update_records:
                        item['_id'] = item['asin']
                    conn.execute(
                        amazon_product.update()
                        .where(amazon_product.c.asin == bindparam('_id'))
                        .values(
                            parent_asin=bindparam('parent_asin'),
                            merchant_id=bindparam('merchant_id'),
                            merchant_name=bindparam('merchant_name'),
                            delivery=bindparam('delivery'),
                            reviews_number=bindparam('reviews_number'),
                            review_score=bindparam('review_score'),
                            seller_number=bindparam('seller_number'),
                            qa_number=bindparam('qa_number'),
                            not_exist=bindparam('not_exist'),
                            status=bindparam('status'),
                            price=bindparam('price'),
                            shipping_weight=bindparam('shipping_weight'),
                            img=bindparam('img'),
                            title=bindparam('title'),
                            brand=bindparam('brand'),
                            is_amazon_choice=bindparam('is_amazon_choice'),
                            is_best_seller=bindparam('is_best_seller'),
                            is_prime=bindparam('is_prime'),
                            first_arrival=bindparam('first_arrival'),
                            hy_update_time=bindparam('hy_update_time'),
                            update_time=bindparam('update_time'),
                            imgs=bindparam('imgs'),
                            description=bindparam('description')
                        ),
                        update_records
                    )


def run():
    input_end = NsqInputEndpoint(TOPIC_NAME, 'haiying_crawler', WORKER_NUMBER,  **INPUT_NSQ_CONF)

    server = pipeflow.Server()
    group = server.add_group('main', WORKER_NUMBER)
    group.set_handle(handle, "thread")
    group.add_input_endpoint('input', input_end)

    server.run()
