#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import time

import sys

sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger

"""
数据表，stocks_main_estimation_indexes_mapper 的映射
以该表为主的数据操作，均在此完成
"""


class StocksMainEstimationIndexesMapper:

    def __init__(self):
        pass

    def save_akshare_stock_estimation(self, stock_code, stock_name, exchange_location, exchange_location_mic,
                                      p_day, pe_ttm, pb, ps_ttm, dv_ratio, dv_ttm, total_mv):
        # 插入的SQL
        inserting_sql = """INSERT INTO stocks_main_estimation_indexes (stock_code, stock_name, exchange_location, exchange_location_mic,
                        p_day, pe_ttm, pb, ps_ttm, dividend_yield, dividend_yield_ttm, market_capitalization) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')""" % (
            stock_code, stock_name, exchange_location, exchange_location_mic,
            p_day, pe_ttm, pb, ps_ttm, dv_ratio, dv_ttm, total_mv)
        if 'nan' in inserting_sql:
            # 将python中的空值替换为mysql中的空值
            inserting_sql = inserting_sql.replace(",'nan'", ',NULL')
        db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)

    """
    获取数据库中,某股票信息的某个估值数据的最新日期
    :param stock_code: 股票代码（如 600519）
    :param estimation_index: 估值指标, 默认是滚动市盈率（如 pe_ttm）
    :param exchange_location: 上市地（如 sh, sz, hk）均可
    :param exchange_location_mic: 交易所MIC码（如 XSHG, XSHE，XHKG）均可
    :return, 如
    {'p_day': datetime.date(2023, 6, 16)}
    """

    def get_the_latest_date_of_stock(self, stock_code, exchange_location, exchange_location_mic, estimation_index="pe_ttm"):
        selecting_sql = """SELECT max(p_day) as p_day FROM financial_data.stocks_main_estimation_indexes 
                                    where stock_code = '%s' and exchange_location = '%s' 
                                    and exchange_location_mic = '%s' and %s is not null """ % (
        stock_code, exchange_location, exchange_location_mic, estimation_index)
        selecting_result = db_operator.DBOperator().select_one("financial_data", selecting_sql)
        return selecting_result
