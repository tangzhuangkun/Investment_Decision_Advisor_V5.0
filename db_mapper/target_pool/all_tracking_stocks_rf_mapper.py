#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import time
import sys

sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger

"""
数据表，all_tracking_stocks_rf 的映射
以该表为主的数据操作，均在此完成
"""


class AllTrackingStocksRfMapper:

    def __init__(self):
        pass

    """
    获取全部证券所代码
    """
    def get_all_exchange_locaiton_mics(self):
        selecting_sql = "SELECT DISTINCT exchange_location_mic FROM all_tracking_stocks_rf"
        selecting_result = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        return selecting_result

    """
    获取某证券交易所，需要跟踪的全部股票
    :param, exchange_location_mic, exchange_location_mic: 交易所MIC码（如 XSHG, XSHE，XHKG）均可， 大小写均可
    :return，如 
    [{'stock_code': '00700', 'stock_name': '腾讯控股', 'exchange_location': 'hk', 'exchange_location_mic': 'XHKG'}, 
    {'stock_code': '09633', 'stock_name': '农夫山泉', 'exchange_location': 'hk', 'exchange_location_mic': 'XHKG'}]
    """
    def get_exchange_all_stocks(self, exchange_location_mic):
        selecting_sql = """SELECT DISTINCT stock_code, stock_name, exchange_location, exchange_location_mic 
                                    FROM all_tracking_stocks_rf where exchange_location_mic = '%s' """ % (
            exchange_location_mic)
        all_tracking_stock_dict = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        return all_tracking_stock_dict


if __name__ == '__main__':
    time_start = time.time()
    go = AllTrackingStocksRfMapper()
    #result = go.get_all_exchange_locaiton_mics()
    result = go.get_exchange_all_stocks("XHKG")
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)