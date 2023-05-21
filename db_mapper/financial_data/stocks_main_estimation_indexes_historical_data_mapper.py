#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import decimal
import time

import sys

sys.path.append("..")
import database.db_operator as db_operator



"""
数据表，stocks_main_estimation_indexes_historical_data 的映射
以该表为主的数据操作，均在此完成
"""
class StocksMainEstimationIndexesHistoricalDataMapper:

    def __init__(self):
        pass

    """
    获取股票在历史上具体某一天的估值
    # param: stock_code 股票代码，如 600900
    # param: valuation_method 估值方式，如 pe_ttm--滚动市盈率, pe_ttm_nonrecurring--扣非滚动市盈率, pb--市净率, pb_wo_gw--扣非市净率, ps_ttm--滚动市销率, pcf_ttm--滚动市现率, dividend_yield--股息率,
    # param: p_day, 业务日期，如 2023-05-17
    # return： 
    # 如  {'stock_code': '600900', 'stock_name': '长江电力', 'exchange_location': 'sh', 'exchange_location_mic': 'XSHG', 'dividend_yield': Decimal('0.0372457442164993400000'), 'p_day': datetime.date(2023, 5, 12)}
    """

    def get_stock_historical_date_estimation(self, stock_code, valuation_method, p_day):

        selecting_sql = ""

        # 滚动市盈率
        if (valuation_method == "pe_ttm"):
            selecting_sql = """  select stock_code, stock_name, exchange_location, exchange_location_mic, pe_ttm, `date` as p_day from 
            financial_data.stocks_main_estimation_indexes_historical_data  
            where stock_code = '%s' and `date` = '%s'  """ % (stock_code, p_day)

        # 扣非滚动市盈率
        elif (valuation_method == "pe_ttm_nonrecurring"):
            selecting_sql = """select stock_code, stock_name, exchange_location, exchange_location_mic,  pe_ttm_nonrecurring, `date` as p_day  from 
                        financial_data.stocks_main_estimation_indexes_historical_data  
                        where stock_code = '%s' and `date` = '%s'  """ % (stock_code, p_day)

        # 市净率
        elif (valuation_method == "pb"):
            selecting_sql = """select stock_code, stock_name, exchange_location, exchange_location_mic,  pb, `date` as p_day  from 
                            financial_data.stocks_main_estimation_indexes_historical_data  
                            where stock_code = '%s' and `date` = '%s'  """ % (stock_code, p_day)

        # 扣商誉市净率
        elif (valuation_method == "pb_wo_gw"):
            selecting_sql = """select stock_code, stock_name, exchange_location, exchange_location_mic,  pb_wo_gw, `date` as p_day  from 
                            financial_data.stocks_main_estimation_indexes_historical_data  
                            where stock_code = '%s' and `date` = '%s'  """ % (stock_code, p_day)

        # 滚动市销率
        elif (valuation_method == "ps_ttm"):
            selecting_sql = """select stock_code, stock_name, exchange_location, exchange_location_mic,  ps_ttm, `date` as p_day  from 
                                 financial_data.stocks_main_estimation_indexes_historical_data  
                                 where stock_code = '%s' and `date` = '%s'  """ % (stock_code, p_day)

        # 滚动市销率
        elif (valuation_method == "pcf_ttm"):
            selecting_sql = """select stock_code, stock_name, exchange_location, exchange_location_mic,  pcf_ttm, `date` as p_day  from 
                                 financial_data.stocks_main_estimation_indexes_historical_data  
                                 where stock_code = '%s' and `date` = '%s'  """ % (stock_code, p_day)

        # 股息率
        elif (valuation_method == "dividend_yield"):
            selecting_sql = """select stock_code, stock_name, exchange_location, exchange_location_mic,  dividend_yield, `date` as p_day  from 
                                financial_data.stocks_main_estimation_indexes_historical_data  
                                where stock_code = '%s' and `date` = '%s'  """ % (stock_code, p_day)

        # 其它
        else:
            return None
        stock_estiamtion_info = db_operator.DBOperator().select_one("financial_data", selecting_sql)
        return stock_estiamtion_info


if __name__ == '__main__':
    time_start = time.time()
    go = StocksMainEstimationIndexesHistoricalDataMapper()
    result = go.get_stock_historical_date_estimation("600900", "dividend_yield", "2023-05-12")
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)