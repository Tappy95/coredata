from sqlalchemy import Table, Column, PrimaryKeyConstraint, Integer, String, TIMESTAMP, Boolean
from sqlalchemy.dialects.mysql import TINYINT
from . import metadata


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


amazon_category = Table(
    'amazon_category', metadata,
    Column('category_id', String),
    Column('category_name', String),
    Column('level', TINYINT),
    Column('is_leaf', Boolean),
    Column('parent_id', String),
    Column('category_id_path', String),
    Column('category_name_path', String),
    Column('hy_create_time', TIMESTAMP),
    Column('update_time', TIMESTAMP),
    PrimaryKeyConstraint('category_id_path', name='pk')
)
