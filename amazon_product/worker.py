import re
import json
from datetime import datetime, timedelta
from collections import defaultdict
from sqlalchemy import create_engine
from sqlalchemy.sql import select, and_, bindparam
import pipeflow
from pipeflow import NsqInputEndpoint, NsqOutputEndpoint
from task_protocol import HYTask
from config import *
from api.amazon_product import GetAmazonProductBySearch, GetAmazonProductAdditionalInfo
from models.amazon_models import amazon_product, amazon_product_relationship
from util.pub import pub_to_nsq
from util.log import logger

WORKER_NUMBER = 3
MAX_STATEMENT = 4900
RELATIONSHIP_MIN_UPDATE = timedelta(days=14)
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

    def __init__(self, site, infos):
        self.site = site
        self.infos = infos
        self.time_now = datetime.now()

    def parse_timestr(self, timestr, ptype='last_upd_date'):
        if ptype == 'last_upd_date':
            time_format_str = "%Y-%m-%d %H:%M:%S"
        else:
            time_format_str = "%Y-%m-%d"
        i = timestr.find(".")
        try:
            if i != -1:
                return datetime.strptime(timestr[:i], time_format_str)
            else:
                return datetime.strptime(timestr, time_format_str)
        except:
            return

    def asins(self, batch=100):
        info_cnt = len(self.infos)
        i = 0
        while i < info_cnt:
            yield list(map(lambda x:x["asin"], self.infos[i:i+batch]))
            i += batch

    def parse(self, info):
        parsed_info = {
            "asin": info["asin"],
            "site": self.site,
            "category_ids": ",".join(set([item["cate_id"] for item in info["top_cates"]] +
                                         [item["cate_id"] for item in info["sub_cates"]])),
            "parent_asin": info["parent_asin"] or "",
            "merchant_id": info["merchant_code"] or "",
            "merchant_name": info["merchant"] or "",
            "delivery": self._deliver_map.get(info["delivery"], 3),
            "reviews_number": info["customer_reviews"] or 0,
            "review_score": info["score"] or 0,
            "seller_number": info["follow_sellers_num"] or 0,
            "qa_number": info["answered_questions"] or 0,
            "not_exist": info["not_exist"] or 0,
            "status": info["status"] or 0,
            "price": info["asin_price_min"] or 0,
            "shipping_weight": info["shipping_weight"] or 0,
            "img": info["img_url"] or "",
            "title": info["title"] or "",
            "brand": info["brand"] or "",
            "is_amazon_choice": info["is_ama_choice"] or 0,
            "is_best_seller": info["is_best_seller"] or 0,
            "is_prime": info["is_prime"] or 0,
            "first_arrival": self.parse_timestr(info["fir_arrival"], "fir_arrival") \
                            if info["fir_arrival"] else None,
            "hy_update_time": self.parse_timestr(info["last_upd_date"]),
            "update_time": self.time_now,
            "imgs": ",".join(info["asin_images_url"]),
            "description": info["prod_desc"],
        }
        return parsed_info

    def parsed_infos(self, batch=500):
        info_cnt = len(self.infos)
        i = 0
        while i < info_cnt:
            yield list(map(self.parse, self.infos[i:i+batch]))
            i += batch


class relationshipInfo:

    _relationship_keys = ("bought_together", "bought_also_bought", "viewed_also_viewed",
                          "viewed_also_bought", "sponsored_1", "sponsored_2",
                          "compare_to_similar")

    def __init__(self, site, infos):
        self.site = site
        self.time_now = datetime.now()
        self.infos = []
        for info in infos:
            to_asins = set([])
            i = info["last_upd_date"].find(".")
            try:
                if i != -1:
                    update_time = datetime.strptime(info["last_upd_date"][:i], "%Y-%m-%d %H:%M:%S")
                else:
                    update_time = datetime.strptime(info["last_upd_date"], "%Y-%m-%d %H:%M:%S")
            except:
                update_time = self.time_now
            for k in self._relationship_keys:
                v = info.get(k)
                if v:
                    to_asins.update([item.strip(":") for item in v.split(",")])
            if to_asins:
                self.infos.append({"asin": info["asin"], "to_asins": to_asins,
                                   "update_time": update_time})

    def parsed_infos(self, batch=500):
        info_cnt = len(self.infos)
        i = 0
        while i < info_cnt:
            yield self.infos[i:i+batch]
            i += batch


class HYProductTask(HYTask):

    @property
    def site(self):
        return self.task_data['site']

    @property
    def params(self):
        dct = {}
        if self.task_data.get('asin'):
            dct['asin'] = self.task_data['asin']
        if self.task_data.get('category_id_path'):
            category_ids = self.task_data.get('category_id_path').split(":")
            for i, _id in enumerate(category_ids):
                dct["p_l{}_id".format(i+1)] = _id
        return dct


def handle(group, task):
    hy_task = HYProductTask(task)
    site = hy_task.site
    station = site.upper()
    params = hy_task.params
    current_page = 1
    no_more_data = False
    while not no_more_data:
        logger.info("[request product api] {} {}".format(current_page, hy_task.task_data))
        result = GetAmazonProductBySearch(station, current_page=current_page, **params).request()
        if not result or result["status"] != "success":
            break
        current_page += 1
        no_more_data = result["no_more_data"]
        products = productInfo(site, result.get("result", []))
        additonal_infos = []
        for asins in products.asins():
            result = GetAmazonProductAdditionalInfo(station, asins=asins).request()
            if result and result["status"] == "success":
                additonal_infos.extend(result.get("result", []))
        relationships = relationshipInfo(site, additonal_infos)

        # update product
        with engine.connect() as conn:
            for infos in products.parsed_infos():
                asins = map(lambda x:x["asin"], infos)
                old_records = conn.execute(
                    select([amazon_product.c.asin, amazon_product.c.hy_update_time])
                    .where(
                        and_(
                            amazon_product.c.asin.in_(asins),
                            amazon_product.c.site == site
                        )
                    )
                ).fetchall()
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
                        item['_asin'] = item['asin']
                        item['_site'] = item['site']
                    i = 0
                    while i < len(update_records):
                        conn.execute(
                            amazon_product.update()
                            .where(
                                and_(
                                    amazon_product.c.asin == bindparam('_asin'),
                                    amazon_product.c.site == bindparam('_site')
                                )
                            )
                            .values(
                                category_ids=bindparam('category_ids'),
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
                            update_records[i:i+MAX_STATEMENT]
                        )
                        i += MAX_STATEMENT

        # update product relation
        with engine.connect() as conn:
            for infos in relationships.parsed_infos():
                asins = map(lambda x:x["asin"], infos)
                old_records = conn.execute(
                    select([amazon_product_relationship.c.asin,
                            amazon_product_relationship.c.to_asin,
                            amazon_product_relationship.c.update_time])
                    .where(
                        and_(
                            amazon_product_relationship.c.asin.in_(asins),
                            amazon_product_relationship.c.site == site
                        )
                    )
                ).fetchall()
                old_records_map = defaultdict(dict)
                for item in old_records:
                    old_records_map[item[amazon_product_relationship.c.asin]]\
                            [item[amazon_product_relationship.c.to_asin]] = \
                            item[amazon_product_relationship.c.update_time]
                update_records = []
                add_records = []
                for info in infos:
                    old_to_asins_map = old_records_map.get(info['asin'], {})
                    add_to_asins = info['to_asins'] - set(old_to_asins_map)
                    update_to_asins = info['to_asins'] - add_to_asins
                    add_records.extend([{"to_asin": to_asin, "site": site,
                                         "asin": info["asin"],
                                         "update_time": info["update_time"]}
                                        for to_asin in add_to_asins])
                    update_records.extend([{"_to_asin": to_asin, "_site": site,
                                            "_asin": info["asin"],
                                            "update_time": info["update_time"]}
                                           for to_asin in update_to_asins
                                           if (info["update_time"]-old_to_asins_map[to_asin]) > RELATIONSHIP_MIN_UPDATE])
                if add_records:
                    conn.execute(amazon_product_relationship.insert(), add_records)
                if update_records:
                    i = 0
                    while i < len(update_records):
                        conn.execute(
                            amazon_product_relationship.update()
                            .where(
                                and_(
                                    amazon_product_relationship.c.to_asin == bindparam('_to_asin'),
                                    amazon_product_relationship.c.site == bindparam('_site'),
                                    amazon_product_relationship.c.asin == bindparam('_asin')
                                )
                            )
                            .values(
                                update_time=bindparam('update_time')
                            ),
                            update_records[i:i+MAX_STATEMENT]
                        )
                        i += MAX_STATEMENT


def run():
    input_end = NsqInputEndpoint(TOPIC_NAME, 'haiying_crawler', WORKER_NUMBER,  **INPUT_NSQ_CONF)

    server = pipeflow.Server()
    group = server.add_group('main', WORKER_NUMBER)
    group.set_handle(handle, "thread")
    group.add_input_endpoint('input', input_end)

    server.run()
