import json
from collections import OrderedDict
from .base import BaseAPI


class GetAmazonProductBySearch(BaseAPI):

    api = '/hysj_v4/amazon_api/asin_infos'

    def __init__(self, station, current_page=None, best_rank_flag=None, asins=None,
                 title=None, p_l1_id=None, p_l2_id=None, p_l3_id=None, p_l4_id=None,
                 p_l5_id=None, p_l6_id=None, p_l7_id=None, p_l8_id=None, p_l9_id=None,
                 p_l10_id=None, rank_begin=None, rank_end=None, three_day_rank_begin=None,
                 three_day_rank_end=None, three_day_rank_change_start=None,
                 three_day_rank_change_end=None, three_day_rank_change_rate_start=None,
                 three_day_rank_change_rate_end=None, three_day_new_reviews_begin=None,
                 three_day_new_reviews_end=None, asin_price_begin=None,
                 asin_price_end=None, customer_reviews_begin=None,
                 customer_reviews_end=None, score_begin=None, score_end=None,
                 answered_questions_begin=None, answered_questions_end=None,
                 follow_sellers_num_begin=None, follow_sellers_num_end=None,
                 shipping_weight_begin=None, shipping_weight_end=None,
                 fir_arrival_begin=None, fir_arrival_end=None, best_seller=None,
                 buy_box=None, delivery_type=None, prime=None, amazon_choice=None,
                 sign_of_rank_rise=None, last_upd_date_begin=None, last_upd_date_end=None,
                 rank_rise_avg_change_begin=None, rank_rise_avg_change_end=None,
                 title_type=None, brand=None, merchant=None, price_status=None,
                 chinese_sellers_in_merhants=None, is_registered=None,
                 registration_type=None, order_by=None, order_by_type=None):
        self.param = OrderedDict({
            "station": station, "current_page": current_page,
            "best_rank_flag": best_rank_flag, "asin": ','.join(asins) if asins else None,
            "title": title,
            "p_l1_id": p_l1_id, "p_l2_id": p_l2_id, "p_l3_id": p_l3_id, "p_l4_id": p_l4_id,
            "p_l5_id": p_l5_id, "p_l6_id": p_l6_id, "p_l7_id": p_l7_id, "p_l8_id": p_l8_id,
            "p_l9_id": p_l9_id, "p_l10_id": p_l10_id, "rank_begin": rank_begin,
            "rank_end": rank_end, "three_day_rank_begin": three_day_rank_begin,
            "three_day_rank_end": three_day_rank_end,
            "three_day_rank_change_start": three_day_rank_change_start,
            "three_day_rank_change_end": three_day_rank_change_end,
            "three_day_rank_change_rate_start": three_day_rank_change_rate_start,
            "three_day_rank_change_rate_end": three_day_rank_change_rate_end,
            "three_day_new_reviews_begin": three_day_new_reviews_begin,
            "three_day_new_reviews_end": three_day_new_reviews_end,
            "asin_price_begin": asin_price_begin, "asin_price_end": asin_price_end,
            "customer_reviews_begin": customer_reviews_begin,
            "customer_reviews_end": customer_reviews_end,
            "score_begin": score_begin, "score_end": score_end,
            "answered_questions_begin": answered_questions_begin,
            "answered_questions_end": answered_questions_end,
            "follow_sellers_num_begin": follow_sellers_num_begin,
            "follow_sellers_num_end": follow_sellers_num_end,
            "shipping_weight_begin": shipping_weight_begin,
            "shipping_weight_end": shipping_weight_end,
            "fir_arrival_begin": fir_arrival_begin, "fir_arrival_end": fir_arrival_end,
            "best_seller": best_seller, "buy_box": buy_box, "delivery_type": delivery_type,
            "prime": prime, "amazon_choice": amazon_choice,
            "sign_of_rank_rise": sign_of_rank_rise,
            "last_upd_date_begin": last_upd_date_begin,
            "last_upd_date_end": last_upd_date_end,
            "rank_rise_avg_change_begin": rank_rise_avg_change_begin,
            "rank_rise_avg_change_end": rank_rise_avg_change_end,
            "title_type": title_type, "brand": brand, "merchant": merchant,
            "price_status": price_status,
            "chinese_sellers_in_merhants": chinese_sellers_in_merhants,
            "is_registered": is_registered, "registration_type": registration_type,
            "order_by": order_by, "order_by_type": order_by_type,
        })


class GetAmazonProduct(BaseAPI):

    api = '/hysj_v4/amazon_api/asin_info'

    def __init__(self, station, asins):
        self.param = OrderedDict({
            "station": station,
            "asins": ','.join(asins)
        })


class GetAmazonProductStock(BaseAPI):

    api = '/hysj_v4/amazon_api/asin_his_infos'

    def __init__(self, station, asins, created_date_start=None, created_date_end=None):
        self.param = OrderedDict({
            "station": station,
            "asins": ','.join(asins),
            "created_date_start": created_date_start,
            "created_date_end": created_date_end
        })


class GetAmazonProductStatistic(BaseAPI):

    api = '/hysj_v4/amazon_api/asin_stat_infos'

    def __init__(self, station, asins):
        self.param = OrderedDict({
            "station": station,
            "asins": ','.join(asins),
        })


class GetAmazonProductAdditionalInfo(BaseAPI):

    api = '/hysj_v4/amazon_api/asin_additional_information'

    def __init__(self, station, asins):
        self.param = OrderedDict({
            "station": station,
            "asins": ','.join(asins)
        })
