#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun
import time
import sys


sys.path.append("..")
import database.db_operator as db_operator


class DataMinerCommonStockOperator:
    # 对数据库中股票信息的通用操作

    def __init__(self):
        pass


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


if __name__ == '__main__':
    time_start = time.time()
    go = DataMinerCommonStockOperator()
    result = go.get_stock_latest_estimation_percentile_in_history("600900", "dividend_yield", 5)
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)