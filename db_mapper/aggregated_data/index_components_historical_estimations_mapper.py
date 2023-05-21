#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import decimal
import time

import sys

sys.path.append("..")
import database.db_operator as db_operator


"""
数据表，index_components_historical_estimations 的映射
以该表为主的数据操作，均在此完成
"""
class IndexComponentsHistoricalEstimationMapper:

    def __init__(self):
        pass

    """
    根据指数当前的构成成分，获取在历史上具体某一天的估值
    # param: index_code 指数代码，如 399997
    # param: valuation_method 估值方式，如 pe_ttm--滚动市盈率, pe_ttm_nonrecurring--扣非滚动市盈率, pb--市净率, pb_wo_gw--扣非市净率, ps_ttm--滚动市销率, pcf_ttm--滚动市现率, dividend_yield--股息率,
    # param: p_day, 业务日期，如 2023-05-17
    # return： 
    # 如  {'index_code': '399997', 'index_name': '中证白酒', 'p_day': datetime.date(2023, 5, 12), 'pe_ttm': Decimal('30.257822604')}
    """
    def get_index_historical_date_estimation(self, index_code, valuation_method, p_day):
        selecting_sql = ""
        # 滚动市盈率
        if (valuation_method == "pe_ttm"):
            selecting_sql = """select index_code, index_name, historical_date as p_day,  
            (pe_ttm*100/pe_ttm_effective_weight) as pe_ttm from aggregated_data.index_components_historical_estimations 
            where index_code = '%s' and historical_date = '%s' """ % (index_code, p_day)

        # 扣非滚动市盈率
        elif (valuation_method == "pe_ttm_nonrecurring"):
            selecting_sql = """select index_code, index_name, historical_date as p_day,  
            (pe_ttm_nonrecurring*100/pe_ttm_nonrecurring_effective_weight) as pe_ttm_nonrecurring from aggregated_data.index_components_historical_estimations 
            where index_code = '%s' and historical_date = '%s' """ % (index_code, p_day)

        # 市净率
        elif (valuation_method == "pb"):
            selecting_sql = """select index_code, index_name, historical_date as p_day,  
            (pb*100/pb_effective_weight) as pb from aggregated_data.index_components_historical_estimations 
            where index_code = '%s' and historical_date = '%s' """ % (index_code, p_day)

        # 扣商誉市净率
        elif (valuation_method == "pb_wo_gw"):
            selecting_sql = """select index_code, index_name, historical_date as p_day,  
            (pb_wo_gw*100/pb_wo_gw_effective_weight) as pb_wo_gw from aggregated_data.index_components_historical_estimations 
            where index_code = '%s' and historical_date = '%s' """ % (index_code, p_day)

        # 滚动市销率
        elif (valuation_method == "ps_ttm"):
            selecting_sql = """select index_code, index_name, historical_date as p_day,  
            (ps_ttm*100/ps_ttm_effective_weight) as ps_ttm from aggregated_data.index_components_historical_estimations 
            where index_code = '%s' and historical_date = '%s' """ % (index_code, p_day)

        # 滚动市销率
        elif (valuation_method == "pcf_ttm"):
            selecting_sql = """select index_code, index_name, historical_date as p_day,  
            (pcf_ttm*100/pcf_ttm_effective_weight) as pcf_ttm from aggregated_data.index_components_historical_estimations 
            where index_code = '%s' and historical_date = '%s' """ % (index_code, p_day)

        # 股息率
        elif (valuation_method == "dividend_yield"):
            selecting_sql = """select index_code, index_name, historical_date as p_day,  
            (dividend_yield*100/dividend_yield_effective_weight) as dividend_yield from aggregated_data.index_components_historical_estimations 
            where index_code = '%s' and historical_date = '%s' """ % (index_code, p_day)

        # 其它
        else:
            return None

        index_estiamtion_info = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        return index_estiamtion_info

if __name__ == '__main__':
    time_start = time.time()
    go = IndexComponentsHistoricalEstimationMapper()
    result = go.get_index_historical_date_estimation("399997", "pe_ttm", "2023-05-12")
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)