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
        selecting_sql = "SELECT DISTINCT exchange_location_mic FROM target_pool.all_tracking_stocks_rf"
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
                                    FROM target_pool.all_tracking_stocks_rf where exchange_location_mic = '%s' """ % (
            exchange_location_mic)
        all_tracking_stock_dict = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        return all_tracking_stock_dict

    """
    某交易所，统计所有需跟踪的股票
    :param exchange_location_mic: 交易所MIC码（如 XSHG, XSHE，XHKG）均可， 大小写均可
    :return: 
    如，{'counter': 2}
    """
    def exchagne_all_tracking_stocks_counter(self, exchange_location_mic):
        # 统一处理为大写
        exchange_location_mic = exchange_location_mic.upper()

        selecting_sql = """SELECT count(DISTINCT stock_code) as counter FROM target_pool.all_tracking_stocks_rf 
                            where exchange_location_mic = '%s' """ % (exchange_location_mic)
        stock_codes_counter = db_operator.DBOperator().select_one("target_pool", selecting_sql)
        return stock_codes_counter

    """
    某个交易所，获取数据库中已有的且也是那些需要被跟踪的股票
    :param exchange_location_mic: 交易所MIC码（如 XSHG, XSHE，XHKG）均可
    :param latest_collection_date: stocks_main_estimation_indexes_historical_data收集数据的最新日期， 如 2022-04-03
    :return:
    如
    [{'stock_code': '000858', 'stock_name': '五粮液', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
    {'stock_code': '002714', 'stock_name': '牧原股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},,，，，]
    """
    def get_the_stocks_that_already_in_db_and_need_to_be_tracked(self, exchange_location_mic, latest_collection_date):
        selecting_sql = """ 
                                    select tar.stock_code, tar.stock_name, tar.exchange_location, tar.exchange_location_mic from 
                                    -- 某个交易所，需要跟踪的全部股票
                                    (select distinct stock_code, stock_name, exchange_location, exchange_location_mic 
                                                from target_pool.all_tracking_stocks_rf
                                                where exchange_location_mic = '%s' ) tar
                                    inner join 
                                    -- 数据库中已有的某个交易所的全部股票
                                    (select distinct stock_code, date, exchange_location_mic
                                    from financial_data.stocks_main_estimation_indexes_historical_data
                                    where date = '%s'
                                    and exchange_location_mic = '%s' ) his
                                    on his.stock_code = tar.stock_code
                                    and his.exchange_location_mic = tar.exchange_location_mic """ % (
        exchange_location_mic, latest_collection_date, exchange_location_mic)
        selecting_result = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        return selecting_result

    """
    某个交易所，需要被跟踪,但暂时不在数据库中的股票
    :param exchange_location_mic: 交易所MIC码（如 XSHG, XSHE，XHKG）均可
    :param latest_collection_date: stocks_main_estimation_indexes_historical_data收集数据的最新日期， 如 2022-04-03
    :return:
    如 
    [{'stock_code': '000656', 'stock_name': '金科股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, 
    {'stock_code': '000540', 'stock_name': '中天金融', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, 
    {'stock_code': '000671', 'stock_name': '阳光城', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}]
    """
    def get_the_stocks_that_not_in_db_but_need_to_be_tracked(self, exchange_location_mic, latest_collection_date):
        selecting_sql = """ select tar.stock_code, tar.stock_name, tar.exchange_location, tar.exchange_location_mic from 
                                    -- 某个交易所，需要跟踪的全部股票
                                    (select distinct stock_code, stock_name, exchange_location, exchange_location_mic 
                                                            from target_pool.all_tracking_stocks_rf
                                                            where exchange_location_mic = '%s' ) tar
                                    left join 
                                    -- 数据库中已有的某个交易所的全部股票
                                    (select distinct stock_code, date, exchange_location_mic
                                    from financial_data.stocks_main_estimation_indexes_historical_data
                                    where date = '%s'
                                    and exchange_location_mic = '%s') his
                                    on his.stock_code = tar.stock_code
                                    and his.exchange_location_mic = tar.exchange_location_mic
                                    where his.stock_code is null""" % (
        exchange_location_mic, latest_collection_date, exchange_location_mic)
        selecting_result = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        return selecting_result

    """
    根据分页信息获取,某个交易所，哪些股票需要被收集估值信息
    :param exchange_location_mic: 交易所MIC码（如 XSHG, XSHE，XHKG）均可
    :param start_row: 开始行
    :param size: 每页条数
    """
    def get_paged_exchange_demanded_stocks(self, exchange_location_mic,start_row,size):
        selecting_sql = """SELECT DISTINCT stock_code, stock_name, exchange_location, exchange_location_mic 
                                    FROM target_pool.all_tracking_stocks_rf where exchange_location_mic = '%s' 
                                    order by stock_code limit %d,%d """ % (exchange_location_mic, start_row, size)
        paged_stock_info = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        return paged_stock_info


if __name__ == '__main__':
    time_start = time.time()
    go = AllTrackingStocksRfMapper()
    #result = go.get_all_exchange_locaiton_mics()
    #result = go.get_exchange_all_stocks("XHKG")
    #result = go.exchagne_all_tracking_stocks_counter("XHKG")
    #result = go.get_the_stocks_that_already_in_db_and_need_to_be_tracked("XHKG","2023-06-13")
    #result = go.get_the_stocks_that_not_in_db_but_need_to_be_tracked("XSHE","2023-06-13")
    result = go.get_paged_exchange_demanded_stocks("XSHE",0,10)
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)