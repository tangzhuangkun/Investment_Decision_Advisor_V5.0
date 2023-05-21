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

    """
    根据指数当前的构成成分，获取过去一段时间X年的，XX估值方式，全部估值信息
    # param: index_code 指数代码，如 399997
    # param: valuation_method 估值方式，如 pe_ttm--滚动市盈率, pe_ttm_nonrecurring--扣非滚动市盈率, pb--市净率, pb_wo_gw--扣非市净率, ps_ttm--滚动市销率, pcf_ttm--滚动市现率, dividend_yield--股息率
    # param: p_day, 业务日期，从这个日期往前x年，如 2023-05-17
    # param: years, 过去x年，如 10
    # return： 
    # 如  [{'index_code': '399997', 'index_name': '中证白酒', 'pe_ttm': Decimal('21.825887910'), 'p_day': datetime.date(2018, 10, 30)}, 
            {'index_code': '399997', 'index_name': '中证白酒', 'pe_ttm': Decimal('22.049658433'), 'p_day': datetime.date(2018, 10, 29)}, ,,,]
    """
    def get_index_a_period_estimation(self, index_code, valuation_method, p_day, years):
        selecting_sql = ""
        # 滚动市盈率
        if (valuation_method == "pe_ttm"):
            selecting_sql = """ select index_code, index_name, round(pe_ttm*100/pe_ttm_effective_weight, 4) as pe_ttm,
            historical_date as p_day from aggregated_data.index_components_historical_estimations 
            where index_code = '%s' 
            and historical_date >= date_sub('%s', interval '%s' year)
            order by pe_ttm/(pe_ttm_effective_weight/100) """ % (index_code, p_day, years)

        # 扣非滚动市盈率
        elif (valuation_method == "pe_ttm_nonrecurring"):
            selecting_sql = """ select index_code, index_name, round(pe_ttm_nonrecurring*100/pe_ttm_nonrecurring_effective_weight, 4) as pe_ttm_nonrecurring,
            historical_date as p_day from aggregated_data.index_components_historical_estimations 
            where index_code = '%s' 
            and historical_date >= date_sub('%s', interval '%s' year)
            order by pe_ttm_nonrecurring/(pe_ttm_nonrecurring_effective_weight/100) """ % (index_code, p_day, years)

        # 市净率
        elif (valuation_method == "pb"):
            selecting_sql = """ select index_code, index_name, round(pb*100/pb_effective_weight, 4) as pb,
            historical_date as p_day from aggregated_data.index_components_historical_estimations 
            where index_code = '%s' 
            and historical_date >= date_sub('%s', interval '%s' year)
            order by pb/(pb_effective_weight/100) """ % (index_code, p_day, years)

        # 扣商誉市净率
        elif (valuation_method == "pb_wo_gw"):
            selecting_sql = """ select index_code, index_name, round(pb_wo_gw*100/pb_wo_gw_effective_weight, 4) as pb_wo_gw,
            historical_date as p_day from aggregated_data.index_components_historical_estimations 
            where index_code = '%s' 
            and historical_date >= date_sub('%s', interval '%s' year)
            order by pb_wo_gw/(pb_wo_gw_effective_weight/100) """ % (index_code, p_day, years)

        # 滚动市销率
        elif (valuation_method == "ps_ttm"):
            selecting_sql = """ select index_code, index_name, round(ps_ttm*100/ps_ttm_effective_weight, 4) as ps_ttm,
            historical_date as p_day from aggregated_data.index_components_historical_estimations 
            where index_code = '%s' 
            and historical_date >= date_sub('%s', interval '%s' year)
            order by ps_ttm/(ps_ttm_effective_weight/100) """ % (index_code, p_day, years)

        # 滚动市销率
        elif (valuation_method == "pcf_ttm"):
            selecting_sql = """ select index_code, index_name, round(pcf_ttm*100/pcf_ttm_effective_weight, 4) as pcf_ttm,
            historical_date as p_day from aggregated_data.index_components_historical_estimations 
            where index_code = '%s' 
            and historical_date >= date_sub('%s', interval '%s' year)
            order by pcf_ttm/(pcf_ttm_effective_weight/100) """ % (index_code, p_day, years)

        # 股息率
        elif (valuation_method == "dividend_yield"):
            selecting_sql = """ select index_code, index_name, round(dividend_yield*100/dividend_yield_effective_weight, 4) as dividend_yield,
            historical_date as p_day from aggregated_data.index_components_historical_estimations 
            where index_code = '%s' 
            and historical_date >= date_sub('%s', interval '%s' year)
            order by dividend_yield/(dividend_yield_effective_weight/100) """ % (index_code, p_day, years)

        # 其它
        else:
            return None

        index_estiamtion_info = db_operator.DBOperator().select_all("aggregated_data", selecting_sql)
        return index_estiamtion_info

if __name__ == '__main__':
    time_start = time.time()
    go = IndexComponentsHistoricalEstimationMapper()
    result = go.get_index_a_period_estimation("399997", "pe_ttm", "2023-05-12", 5)
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)