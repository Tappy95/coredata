from sqlalchemy import Table, Column, PrimaryKeyConstraint, Integer, String, TIMESTAMP,\
    Boolean, TEXT, DECIMAL, DateTime
from sqlalchemy.dialects.mysql import TINYINT
from models import metadata
import datetime

amazon_keyword_rank = Table(
    'amazon_keyword_rank', metadata,
    Column('asin', String),
    Column('keyword', String),
    Column('site', String),
    Column('rank', Integer),
    Column('aid', String),
    Column('update_time', TIMESTAMP),
    PrimaryKeyConstraint('asin', 'keyword', 'site', name='pk')
)

amazon_keyword_task = Table(
    'amazon_keyword_task', metadata,
    Column('asin', String),
    Column('id', String),
    Column('keyword', String),
    Column('capture_status', String),
    Column('status', String),
    Column('monitoring_num', Integer),
    Column('monitoring_count', Integer),
    Column('start_time', TIMESTAMP),
    Column('created_at', TIMESTAMP),
    Column('end_time', TIMESTAMP),
    Column('monitoring_type', String),
    Column('deleted_at', TIMESTAMP),
    Column('phone_num', Integer),
    Column('station', String),
    Column('is_add', String),
    PrimaryKeyConstraint('id', name='pk')
)


amazon_category = Table(
    'amazon_category', metadata,
    Column('category_id', String),
    Column('category_name', String),
    Column('level', TINYINT),
    Column('is_leaf', Boolean),
    Column('parent_id', String),
    Column('site', String),
    Column('category_id_path', String),
    Column('category_name_path', String),
    Column('hy_create_time', TIMESTAMP),
    Column('update_time', TIMESTAMP),
    PrimaryKeyConstraint('category_id_path', 'site', name='pk')
)


amazon_product = Table(
    'amazon_product', metadata,
    Column('asin', String),
    Column('site', String),
    Column('parent_asin', String),
    Column('category_ids', String),
    Column('merchant_id', String),
    Column('merchant_name', String),
    Column('delivery', TINYINT),
    Column('reviews_number', Integer),
    Column('review_score', DECIMAL(8,2)),
    Column('seller_number', Integer),
    Column('qa_number', Integer),
    Column('not_exist', TINYINT),
    Column('status', TINYINT),
    Column('price', DECIMAL(8,2)),
    Column('shipping_weight', DECIMAL(8,2)),
    Column('img', String),
    Column('title', String),
    Column('brand', String),
    Column('is_amazon_choice', TINYINT),
    Column('is_best_seller', TINYINT),
    Column('is_prime', TINYINT),
    Column('first_arrival', TIMESTAMP),
    Column('hy_update_time', TIMESTAMP),
    Column('update_time', TIMESTAMP),
    Column('imgs', String),
    Column('description', TEXT),
    PrimaryKeyConstraint('asin', 'site', name='pk')
)
#listing_asins
#chinese_sellers
#chinese_sellers_in_merhants
#is_registered
#registration


amazon_product_relationship = Table(
    'amazon_product_relationship', metadata,
    Column('to_asin', String),
    Column('site', String),
    Column('asin', String),
    Column('update_time', TIMESTAMP),
    PrimaryKeyConstraint('to_asin', 'site', 'asin',  name='pk')
)
