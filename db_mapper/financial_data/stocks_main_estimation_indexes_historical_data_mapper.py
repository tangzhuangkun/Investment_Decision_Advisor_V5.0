#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import time

import sys

sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger



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
            # 日志记录
            log_msg = '估值方式错误，获取 ' + stock_code + '在 ' + p_day + '的' + valuation_method + '估值 ' + '，失败'
            custom_logger.CustomLogger().log_writter(log_msg, 'error')
            return None
        stock_estiamtion_info = db_operator.DBOperator().select_one("financial_data", selecting_sql)
        # 日志记录
        log_msg = '获取 ' + stock_code + '在 ' + p_day + '的' + valuation_method + '估值 ' + '，成功'
        custom_logger.CustomLogger().log_writter(log_msg, 'debug')
        return stock_estiamtion_info


    """
    获取股票最新某估值在过去X年的百分比
    # param: stock_code 股票代码，如 600900
    # param: valuation_method 估值方式，如 pe_ttm--滚动市盈率, pe_ttm_nonrecurring--扣非滚动市盈率, pb--市净率, pb_wo_gw--扣非市净率, ps_ttm--滚动市销率, pcf_ttm--滚动市现率, dividend_yield--股息率,
    # param: years 年数，如 10
    # 返回： 
    # 如 {'stock_code': '600900', 'stock_name': '长江电力', 'latest_date': datetime.date(2023, 5, 12), 'dividend_yield': Decimal('0.0372'), 'row_num': 645, 'total_record': 1198, 'percentage': 53.8, 'previous_year_num': '5'}
    """
    def get_stock_latest_estimation_percentile_in_history(self, stock_code, valuation_method, years):
        selecting_sql = ""

        # 滚动市盈率
        if (valuation_method == "pe_ttm"):
            selecting_sql = """  select raw.stock_code, raw.stock_name, raw.date as latest_date, round(raw.pe_ttm, 4) as pe_ttm, raw.row_num, record.total_record, round(raw.percent_num*100, 2 ) as percentage, '%s' as previous_year_num from 
                            (select stock_code, stock_name, date, pe_ttm,
                            row_number() OVER (partition by stock_code ORDER BY pe_ttm asc) AS row_num,  
                            percent_rank() OVER (partition by stock_code ORDER BY pe_ttm asc) AS percent_num
                            from stocks_main_estimation_indexes_historical_data
                            where stock_code = '%s'
                            and `date` > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                            left join 
                            (select stock_code, count(1) as total_record from stocks_main_estimation_indexes_historical_data where stock_code = '%s' and `date` > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by stock_code) as record
                            on raw.stock_code = record.stock_code
                            where raw.date = (select max(date) from stocks_main_estimation_indexes_historical_data)
                            order by raw.percent_num asc """  % (years, stock_code, years, stock_code, years)

        # 扣非滚动市盈率
        elif (valuation_method == "pe_ttm_nonrecurring"):
            selecting_sql = """  select raw.stock_code, raw.stock_name, raw.date as latest_date, round(raw.pe_ttm_nonrecurring, 4) as pe_ttm_nonrecurring, raw.row_num, record.total_record, round(raw.percent_num*100, 2 ) as percentage, '%s' as previous_year_num from 
                                (select stock_code, stock_name, date, pe_ttm_nonrecurring,
                                row_number() OVER (partition by stock_code ORDER BY pe_ttm_nonrecurring asc) AS row_num,  
                                percent_rank() OVER (partition by stock_code ORDER BY pe_ttm_nonrecurring asc) AS percent_num
                                from stocks_main_estimation_indexes_historical_data
                                where stock_code = '%s'
                                and `date` > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                                left join 
                                (select stock_code, count(1) as total_record from stocks_main_estimation_indexes_historical_data where stock_code = '%s' and `date` > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by stock_code) as record
                                on raw.stock_code = record.stock_code
                                where raw.date = (select max(date) from stocks_main_estimation_indexes_historical_data)
                                order by raw.percent_num asc """ % (years, stock_code, years, stock_code, years)

        # 市净率
        elif (valuation_method == "pb"):
            selecting_sql = """  select raw.stock_code, raw.stock_name, raw.date as latest_date, round(raw.pb, 4) as pb, raw.row_num, record.total_record, round(raw.percent_num*100, 2 ) as percentage, '%s' as previous_year_num from 
                                (select stock_code, stock_name, date, pb,
                                row_number() OVER (partition by stock_code ORDER BY pb asc) AS row_num,  
                                percent_rank() OVER (partition by stock_code ORDER BY pb asc) AS percent_num
                                from stocks_main_estimation_indexes_historical_data
                                where stock_code = '%s'
                                and `date` > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                                left join 
                                (select stock_code, count(1) as total_record from stocks_main_estimation_indexes_historical_data where stock_code = '%s' and `date` > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by stock_code) as record
                                on raw.stock_code = record.stock_code
                                where raw.date = (select max(date) from stocks_main_estimation_indexes_historical_data)
                                order by raw.percent_num asc """ % (years, stock_code, years, stock_code, years)

        # 扣商誉市净率
        elif (valuation_method == "pb_wo_gw"):
            selecting_sql = """  select raw.stock_code, raw.stock_name, raw.date as latest_date, round(raw.pb_wo_gw, 4) as pb_wo_gw, raw.row_num, record.total_record, round(raw.percent_num*100, 2 ) as percentage, '%s' as previous_year_num from 
                                    (select stock_code, stock_name, date, pb_wo_gw,
                                    row_number() OVER (partition by stock_code ORDER BY pb_wo_gw asc) AS row_num,  
                                    percent_rank() OVER (partition by stock_code ORDER BY pb_wo_gw asc) AS percent_num
                                    from stocks_main_estimation_indexes_historical_data
                                    where stock_code = '%s'
                                    and `date` > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                                    left join 
                                    (select stock_code, count(1) as total_record from stocks_main_estimation_indexes_historical_data where stock_code = '%s' and `date` > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by stock_code) as record
                                    on raw.stock_code = record.stock_code
                                    where raw.date = (select max(date) from stocks_main_estimation_indexes_historical_data)
                                    order by raw.percent_num asc """ % (years, stock_code, years, stock_code, years)

        # 滚动市销率
        elif (valuation_method == "ps_ttm"):
            selecting_sql = """  select raw.stock_code, raw.stock_name, raw.date as latest_date, round(raw.ps_ttm, 4) as ps_ttm, raw.row_num, record.total_record, round(raw.percent_num*100, 2 ) as percentage, '%s' as previous_year_num from 
                                    (select stock_code, stock_name, date, ps_ttm,
                                    row_number() OVER (partition by stock_code ORDER BY ps_ttm asc) AS row_num,  
                                    percent_rank() OVER (partition by stock_code ORDER BY ps_ttm asc) AS percent_num
                                    from stocks_main_estimation_indexes_historical_data
                                    where stock_code = '%s'
                                    and `date` > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                                    left join 
                                    (select stock_code, count(1) as total_record from stocks_main_estimation_indexes_historical_data where stock_code = '%s' and `date` > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by stock_code) as record
                                    on raw.stock_code = record.stock_code
                                    where raw.date = (select max(date) from stocks_main_estimation_indexes_historical_data)
                                    order by raw.percent_num asc """ % (years, stock_code, years, stock_code, years)

        # 滚动市销率
        elif (valuation_method == "pcf_ttm"):
            selecting_sql = """  select raw.stock_code, raw.stock_name, raw.date as latest_date, round(raw.pcf_ttm, 4) as pcf_ttm, raw.row_num, record.total_record, round(raw.percent_num*100, 2 ) as percentage, '%s' as previous_year_num from 
                                    (select stock_code, stock_name, date, pcf_ttm,
                                    row_number() OVER (partition by stock_code ORDER BY pcf_ttm asc) AS row_num,  
                                    percent_rank() OVER (partition by stock_code ORDER BY pcf_ttm asc) AS percent_num
                                    from stocks_main_estimation_indexes_historical_data
                                    where stock_code = '%s'
                                    and `date` > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                                    left join 
                                    (select stock_code, count(1) as total_record from stocks_main_estimation_indexes_historical_data where stock_code = '%s' and `date` > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by stock_code) as record
                                    on raw.stock_code = record.stock_code
                                    where raw.date = (select max(date) from stocks_main_estimation_indexes_historical_data)
                                    order by raw.percent_num asc """ % (years, stock_code, years, stock_code, years)

        # 股息率
        elif (valuation_method == "dividend_yield"):
            selecting_sql = """  select raw.stock_code, raw.stock_name, raw.date as latest_date, round(raw.dividend_yield, 4) as dividend_yield, raw.row_num, record.total_record, round(raw.percent_num*100, 2 ) as percentage, '%s' as previous_year_num from 
                                    (select stock_code, stock_name, date, dividend_yield,
                                    row_number() OVER (partition by stock_code ORDER BY dividend_yield asc) AS row_num,  
                                    percent_rank() OVER (partition by stock_code ORDER BY dividend_yield asc) AS percent_num
                                    from stocks_main_estimation_indexes_historical_data
                                    where stock_code = '%s'
                                    and `date` > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                                    left join 
                                    (select stock_code, count(1) as total_record from stocks_main_estimation_indexes_historical_data where stock_code = '%s' and `date` > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by stock_code) as record
                                    on raw.stock_code = record.stock_code
                                    where raw.date = (select max(date) from stocks_main_estimation_indexes_historical_data)
                                    order by raw.percent_num asc """ % (years, stock_code, years, stock_code, years)

        # 其它
        else:
            return None
        stock_estiamtion_info = db_operator.DBOperator().select_one("financial_data", selecting_sql)
        return stock_estiamtion_info


    """
    获取股票在历史上的全部估值
    # param: stock_code 股票代码，如 600900
    # param: valuation_method 估值方式，如 pe_ttm--滚动市盈率, pe_ttm_nonrecurring--扣非滚动市盈率, pb--市净率, pb_wo_gw--扣非市净率, ps_ttm--滚动市销率, pcf_ttm--滚动市现率, dividend_yield--股息率,
    # return： 
    # 如  [{'stock_code': '600900', 'stock_name': '长江电力', 'exchange_location': 'sh', 'exchange_location_mic': 'XSHG', 'pb_wo_gw': Decimal('1.1933534324891537000000'), 'p_day': datetime.date(2014, 5, 7)}, 
            {'stock_code': '600900', 'stock_name': '长江电力', 'exchange_location': 'sh', 'exchange_location_mic': 'XSHG', 'pb_wo_gw': Decimal('1.1954544068421276000000'), 'p_day': datetime.date(2014, 4, 30)},,,,]
    """

    def get_stock_all_historical_estimation(self, stock_code, valuation_method):

        selecting_sql = """ select stock_code, stock_name, exchange_location, exchange_location_mic, %s, `date` as p_day from 
            financial_data.stocks_main_estimation_indexes_historical_data  
            where stock_code = '%s' order by %s  """ % (valuation_method, stock_code, valuation_method)
        try:
            stock_estimation_info = db_operator.DBOperator().select_all("financial_data", selecting_sql)
            # 日志记录
            # Todo 此日志被多次触发，需要排查
            log_msg = '获取 ' + stock_code + ' 全部历史 ' + valuation_method + '估值 ' + '，成功'
            custom_logger.CustomLogger().log_writter(log_msg, 'debug')
            return stock_estimation_info
        except Exception as e:
            # 日志记录
            log_msg = '估值方式错误，获取 ' + stock_code + '的全部历史 ' + valuation_method + '估值 ' + '，失败' + str(e)
            custom_logger.CustomLogger().log_writter(log_msg, 'error')
            return None



if __name__ == '__main__':
    time_start = time.time()
    go = StocksMainEstimationIndexesHistoricalDataMapper()
    #result = go.get_stock_latest_estimation_percentile_in_history("600900", "pe_ttm", 5)
    #result = go.get_stock_historical_date_estimation("600900", "pe_ttm", "2023-05-12")
    result = go.get_stock_all_historical_estimation("600900", "pb_wo_gw")
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)