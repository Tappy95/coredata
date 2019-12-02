import json
from collections import OrderedDict
from .base import BaseAPI


class AddAmazonKWM(BaseAPI):

    api = '/hysj_v2/amazon_api/add_mon_kw_asins'

    def __init__(self, station, asin_and_keywords, num_of_days, monitoring_num):
        self.param = OrderedDict({
            "station": station,
            "asin_and_keywords": json.dumps(asin_and_keywords),
            "num_of_days": str(num_of_days),
            "monitoring_num": str(monitoring_num),
        })


class DelAmazonKWM(BaseAPI):

    api = '/hysj_v2/amazon_api/del_mon_kw_asins'

    def __init__(self, ids):
        self.param = OrderedDict({
            "ids": ','.join(ids)
        })


class GetAmazonKWMStatus(BaseAPI):

    api = '/hysj_v2/amazon_api/show_asin_list'

    def __init__(self, station='', capture_status='', ids=[]):
        self.param = OrderedDict({
            "station": station,
            "capture_status": capture_status,
            "monitoring_type": '3',
            "ids": ','.join(ids),
        })


class GetAmazonKWMResult(BaseAPI):

    api = '/hysj_v2/amazon_api/show_kw_asins'

    def __init__(self, ids, start_time='', end_time=''):
        self.param = OrderedDict({
            "ids": ','.join(ids),
            "start_time": start_time,
            "end_time": end_time,
        })


class GetAmazonKWMAllResultToday(BaseAPI):

    api = '/hysj_v2/amazon_api/show_all_kw_asin'

    def __init__(self, station):
        self.param = OrderedDict({
            "station": station,
        })
