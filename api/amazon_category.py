import json
from collections import OrderedDict
from .base import BaseAPI


class GetAmazonCategory(BaseAPI):

    api = '/hysj_v4/amazon_api/cate_tree'

    def __init__(self, station):
        self.param = OrderedDict({
            "station": station
        })
