#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import time

import sys

sys.path.append("..")
import database.db_operator as db_operator


"""
数据表，index_estimation_from_lxr_di 的映射
以该表为主的数据操作，均在此完成
"""

class IndexEstimationFromLXRDiMapper:

    def __init__(self):
        pass


    """
    获取沪深指数300最新某估值在过去X年的百分比
    # param: index_code 指数代码，如 000300
    # param: valuation_method 估值方式，如 pe_ttm--滚动市盈率, pb--市净率, ps_ttm--滚动市销率, dividend_yield--股息率
    # param: years 年数，如 10
    # 返回： 
    # 如 {'index_code': '000300', 'index_name': '沪深300', 'latest_date': datetime.date(2023, 5, 18), 'pe_ttm': Decimal('12.1596'), 'row_num': 527, 'total_num': 1214, 'percentage': 43.36, 'previous_year_num': '5', 'index_code_with_init': 'sh000300'}
    """

    def get_hz_three_hundred_index_latest_estimation_percentile_in_history(self, index_code, valuation_method, years):
        selecting_sql = ""
        # 滚动市盈率, 滚动市盈率市值加权，所有样品公司市值之和 / 所有样品公司归属于母公司净利润之和
        if (valuation_method == "pe_ttm"):
            selecting_sql = """select raw.index_code, raw.index_name, raw.latest_date, round(raw.pe_ttm_mcw, 4) as pe_ttm, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as                                previous_year_num, concat("sh", raw.index_code) as index_code_with_init from 
                                        (select index_code, index_name, trading_date as latest_date, pe_ttm_mcw,
                                        row_number() OVER (partition by index_code ORDER BY pe_ttm_mcw asc) AS row_num,  
                                        percent_rank() OVER (partition by index_code ORDER BY pe_ttm_mcw asc) AS percent_num
                                        from financial_data.index_estimation_from_lxr_di
                                        where index_code = '%s'
                                        and trading_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                                        left join 
                                        (select index_code, count(1) as total_num from financial_data.index_estimation_from_lxr_di where index_code = '%s' and trading_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
                                        on raw.index_code = record.index_code
                                        where raw.latest_date = (select max(trading_date) from financial_data.index_estimation_from_lxr_di)
                                        order by raw.percent_num asc """ % (years, index_code, years, index_code, years)
        # 市净率，市净率市值加权，所有样品公司市值之和 / 净资产之和
        elif (valuation_method == "pb"):
            selecting_sql = """select raw.index_code, raw.index_name, raw.latest_date, round(raw.pb_mcw, 4) as pe_ttm, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as                                previous_year_num, concat("sh", raw.index_code) as index_code_with_init from 
                                                                (select index_code, index_name, trading_date as latest_date, pb_mcw,
                                                                row_number() OVER (partition by index_code ORDER BY pb_mcw asc) AS row_num,  
                                                                percent_rank() OVER (partition by index_code ORDER BY pb_mcw asc) AS percent_num
                                                                from financial_data.index_estimation_from_lxr_di
                                                                where index_code = '%s'
                                                                and trading_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                                                                left join 
                                                                (select index_code, count(1) as total_num from financial_data.index_estimation_from_lxr_di where index_code = '%s' and trading_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
                                                                on raw.index_code = record.index_code
                                                                where raw.latest_date = (select max(trading_date) from financial_data.index_estimation_from_lxr_di)
                                                                order by raw.percent_num asc """ % (
            years, index_code, years, index_code, years)
        # 滚动市销率, 市销率市值加权，所有样品公司市值之和 / 营业额之和
        elif (valuation_method == "ps_ttm"):
            selecting_sql = """select raw.index_code, raw.index_name, raw.latest_date, round(raw.ps_ttm_mcw, 4) as pe_ttm, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as                                previous_year_num, concat("sh", raw.index_code) as index_code_with_init from 
                                                                (select index_code, index_name, trading_date as latest_date, ps_ttm_mcw,
                                                                row_number() OVER (partition by index_code ORDER BY ps_ttm_mcw asc) AS row_num,  
                                                                percent_rank() OVER (partition by index_code ORDER BY ps_ttm_mcw asc) AS percent_num
                                                                from financial_data.index_estimation_from_lxr_di
                                                                where index_code = '%s'
                                                                and trading_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                                                                left join 
                                                                (select index_code, count(1) as total_num from financial_data.index_estimation_from_lxr_di where index_code = '%s' and trading_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
                                                                on raw.index_code = record.index_code
                                                                where raw.latest_date = (select max(trading_date) from financial_data.index_estimation_from_lxr_di)
                                                                order by raw.percent_num asc """ % (
            years, index_code, years, index_code, years)

        # 股息率, 股息率市值加权，所有样品公司市值之和 / 分红之和
        elif (valuation_method == "dividend_yield"):
            selecting_sql = """select raw.index_code, raw.index_name, raw.latest_date, round(raw.dyr_mcw, 4) as pe_ttm, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as                                previous_year_num, concat("sh", raw.index_code) as index_code_with_init from 
                                                                (select index_code, index_name, trading_date as latest_date, dyr_mcw,
                                                                row_number() OVER (partition by index_code ORDER BY dyr_mcw asc) AS row_num,  
                                                                percent_rank() OVER (partition by index_code ORDER BY dyr_mcw asc) AS percent_num
                                                                from financial_data.index_estimation_from_lxr_di
                                                                where index_code = '%s'
                                                                and trading_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                                                                left join 
                                                                (select index_code, count(1) as total_num from financial_data.index_estimation_from_lxr_di where index_code = '%s' and trading_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
                                                                on raw.index_code = record.index_code
                                                                where raw.latest_date = (select max(trading_date) from financial_data.index_estimation_from_lxr_di)
                                                                order by raw.percent_num asc """ % (
            years, index_code, years, index_code, years)

        # 其它
        else:
            return None
        index_estiamtion_info = db_operator.DBOperator().select_one("financial_data", selecting_sql)
        return index_estiamtion_info


if __name__ == '__main__':
    time_start = time.time()
    go = IndexEstimationFromLXRDiMapper()
    # index_constitute_stocks_weight = go.get_index_constitute_stocks('399997')
    # print(index_constitute_stocks_weight)
    # index_name = go.get_index_name("399997")
    # print(index_name)
    # result = go.get_today_updated_index_info()
    # print(result)
    #result = go.get_index_latest_estimation_percentile_in_history("399986", "pb_wo_gw", 5)
    result = go.get_hz_three_hundred_index_latest_estimation_percentile_in_history("000300", "pe_ttm", 8)
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)