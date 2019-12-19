from sqlalchemy import Table, Column, PrimaryKeyConstraint, Integer, String, TIMESTAMP,\
    Boolean, TEXT, DECIMAL
from sqlalchemy.dialects.mysql import TINYINT
from models import metadata


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
    Column('station', String),
    Column('status', String),
    Column('monitoring_num', Integer),
    Column('monitoring_count', Integer),
    Column('monitoring_type', String),
    Column('start_time', TIMESTAMP),
    Column('end_time', TIMESTAMP),
    Column('created_at', TIMESTAMP),
    Column('deleted_at', TIMESTAMP),
    Column('is_add', String),
    Column('last_update', String),
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
    Column('asin', String(16), nullable=False, default=''),
    Column('site', String(8), nullable=False, default=''),
    Column('parent_asin', String(16), nullable=False, default=''),
    Column('category_ids', String(128), nullable=False, default=''),
    Column('merchant_id', String(32), nullable=False, default=''),
    Column('merchant_name', String(512), nullable=False, default=''),
    Column('delivery', TINYINT, nullable=False, default=0),
    Column('reviews_number', Integer, nullable=False, default=0),
    Column('review_score', DECIMAL(8,2), nullable=False, default=0),
    Column('seller_number', Integer, nullable=False, default=0),
    Column('qa_number', Integer, nullable=False, default=0),
    Column('not_exist', TINYINT, nullable=False, default=0),
    Column('status', TINYINT, nullable=False, default=0),
    Column('price', DECIMAL(8,2), nullable=False, default=0),
    Column('shipping_weight', DECIMAL(8,2), nullable=False, default=0),
    Column('img', String(128), nullable=False, default=''),
    Column('title', String(512), nullable=False, default=''),
    Column('brand', String(512), nullable=False, default=''),
    Column('is_amazon_choice', TINYINT, nullable=False, default=0),
    Column('is_best_seller', TINYINT, nullable=False, default=0),
    Column('is_prime', TINYINT, nullable=False, default=0),
    Column('first_arrival', TIMESTAMP, nullable=True),
    Column('hy_update_time', TIMESTAMP, nullable=True),
    Column('update_time', TIMESTAMP, nullable=True),
    Column('imgs', TEXT, nullable=True),
    Column('description', TEXT, nullable=True),
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
