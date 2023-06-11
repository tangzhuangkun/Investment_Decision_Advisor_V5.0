#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import time
import threading

import sys
sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger

"""
数据表，mix_top10_with_bottom_no_repeat 的映射
以该表为主的数据操作，均在此完成
"""
class MixTop10WithBottomNoRepeatMapper:

    def __init__(self):
        pass

    """
    获取数据库中的指数最新的构成股和比例
    :param index_code 指数代码，如 399997
    :return 
    如，[{'stock_code': '000568', 'stock_code_with_init': 'sz000568', 'stock_name': '泸州老窖', 
    'weight': Decimal('15.032706411640243000'), 'stock_exchange_location': 'sz', 'stock_market_code': 'XSHE', 
    'p_day': datetime.date(2022, 4, 1)}, 
    {'stock_code': '000596', 'stock_code_with_init': 'sz000596', 'stock_name': '古井贡酒', 
    'weight': Decimal('3.038864036791435500'), 'stock_exchange_location': 'sz', 'stock_market_code': 'XSHE', 
    'p_day': datetime.date(2022, 4, 1)}, ,,,]
    """
    def get_index_constitute_stocks(self, index_code):
        selecting_sql = """SELECT stock_code, concat(stock_exchange_location,stock_code) as stock_code_with_init, stock_name, weight, stock_exchange_location, stock_market_code, p_day 
        FROM mix_top10_with_bottom_no_repeat 
        WHERE index_code = '%s' """ % (index_code)
        index_constitute_stocks_weight = db_operator.DBOperator().select_all("financial_data", selecting_sql)
        return index_constitute_stocks_weight


if __name__ == '__main__':
    time_start = time.time()
    go = MixTop10WithBottomNoRepeat()
    result = go.get_index_constitute_stocks("399997")
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)