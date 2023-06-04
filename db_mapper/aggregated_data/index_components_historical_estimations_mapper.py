#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import decimal
import time

import sys

sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger

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
            # 日志记录
            log_msg = '估值方式错误，无法获取 ' + index_code + '在 ' + p_day + '的估值 ' + valuation_method + '，失败'
            custom_logger.CustomLogger().log_writter(log_msg, 'error')
            return None

        # 查询
        index_estiamtion_info = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        # 日志记录
        log_msg = '获取 ' + index_code + '在 ' + p_day + '的估值 ' + valuation_method + '，成功'
        custom_logger.CustomLogger().log_writter(log_msg, 'debug')
        return index_estiamtion_info

    """
    根据指数当前的构成成分，获取过去一段时间X年的，XX估值方式，全部估值信息, 从小到大排序
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
            # 日志记录
            log_msg = '估值方式错误，无法获取 ' + index_code + '在 ' + p_day + '之前' + str(
                years) + '年的估值 ' + valuation_method + '列表，失败'
            custom_logger.CustomLogger().log_writter(log_msg, 'error')
            return None

        index_estiamtion_info = db_operator.DBOperator().select_all("aggregated_data", selecting_sql)
        # 日志记录
        log_msg = '获取 ' + index_code + '在 ' + p_day + '之前' + str(years) + '年的估值 ' + valuation_method + '列表，成功'
        custom_logger.CustomLogger().log_writter(log_msg, 'debug')
        return index_estiamtion_info

    """
    获取当前指数上一个交易日的扣非市盈率
    # param: index_code 指数代码，如 399997
    # param: p_day 业务日期，如 2023-05-31
    # param: last_trading_date 上一个交易日期，如 2023-05-30
    # return: 31.308918575
    """
    def get_last_trading_date_pe_ttm_nonrecurring(self, index_code, p_day, last_trading_date):
        selecting_sql = """ select (pe_ttm_nonrecurring * 100 /pe_ttm_nonrecurring_effective_weight) as pe_ttm_nonrecurring 
                            from index_components_historical_estimations 
                            where index_code = '%s' 
                            and historical_date = '%s' """ % (index_code, last_trading_date)
        pe_ttm_nonrecurring_info = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        # 如果pe_info不为空
        if pe_ttm_nonrecurring_info is not None:
            return pe_ttm_nonrecurring_info["pe_ttm_nonrecurring"]
        else:
            # 日志记录
            log_msg = "无法获取日期 " + p_day + " 的上一个交易日 " + last_trading_date + " 的扣非市盈率数据"
            custom_logger.CustomLogger().log_writter(log_msg, 'error')
            return 100000


    """
    获取当前指数上一个交易日的扣商誉市净率
    # param: index_code 指数代码，如 399986
    # param: p_day 业务日期，如 2023-05-31
    # param: last_trading_date 上一个交易日期，如 2023-05-30
    # return: 0.614472774
    """
    def get_last_trading_date_pb_wo_gw(self, index_code, p_day, last_trading_date):
        selecting_sql = """ select (pb_wo_gw * 100 /pb_wo_gw_effective_weight) as pb_wo_gw 
                            from index_components_historical_estimations 
                            where index_code = '%s' 
                            and historical_date = '%s' """ % (index_code, last_trading_date)
        pb_wo_gw_info = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        # 如果pb_wo_gw_info不为空
        if pb_wo_gw_info is not None:
            return pb_wo_gw_info["pb_wo_gw"]
        else:
            # 日志记录
            log_msg = "无法获取日期 " + p_day + " 的上一个交易日 " + last_trading_date + " 的扣商誉市净率"
            custom_logger.CustomLogger().log_writter(log_msg, 'error')
            return 100000


    """
    获取指数最新某估值在过去X年的百分比
    # param: index_code 指数代码，如 399997
    # param: valuation_method 估值方式，如 pe_ttm--滚动市盈率, pe_ttm_nonrecurring--扣非滚动市盈率, pb--市净率, pb_wo_gw--扣非市净率, ps_ttm--滚动市销率, pcf_ttm--滚动市现率, dividend_yield--股息率,
    # param: years 年数，如 10
    # 返回： 
    # 如 {'index_code': '399997', 'index_name': '中证白酒', 'latest_date': datetime.date(2022, 4, 8), 'pe_ttm_effective': Decimal('46.437202040'), 'row_num': 1757, 'total_num': 2169, 'percentage': 81.0}
    """
    def get_index_latest_estimation_percentile_in_history(self, index_code, valuation_method, years):
        selecting_sql = ""
        # 滚动市盈率
        if (valuation_method == "pe_ttm"):
            selecting_sql = """  select raw.index_code, raw.index_name, raw.latest_date, round(raw.pe_ttm_effective, 4) as pe_ttm_effective, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as previous_year_num from 
                                (select index_code, index_name, historical_date as latest_date, pe_ttm*100/pe_ttm_effective_weight as pe_ttm_effective,
                                row_number() OVER (partition by index_code ORDER BY pe_ttm*100/pe_ttm_effective_weight asc) AS row_num,  
                                percent_rank() OVER (partition by index_code ORDER BY pe_ttm*100/pe_ttm_effective_weight asc) AS percent_num
                                from aggregated_data.index_components_historical_estimations
                                where index_code = '%s'
                                and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                                left join 
                                (select index_code, count(1) as total_num from aggregated_data.index_components_historical_estimations where index_code = '%s' and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
                                on raw.index_code = record.index_code
                                where raw.latest_date = (select max(historical_date) from aggregated_data.index_components_historical_estimations)
                                order by raw.percent_num asc """ % (years, index_code, years, index_code, years)
        # 扣非滚动市盈率
        elif (valuation_method == "pe_ttm_nonrecurring"):
            selecting_sql = """  select raw.index_code, raw.index_name, raw.latest_date, round(raw.pe_ttm_nonrecurring_effective, 4) as pe_ttm_nonrecurring_effective, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as previous_year_num  from 
                					(select index_code, index_name, historical_date as latest_date, pe_ttm_nonrecurring*100/pe_ttm_nonrecurring_effective_weight as pe_ttm_nonrecurring_effective,
                					row_number() OVER (partition by index_code ORDER BY pe_ttm_nonrecurring*100/pe_ttm_nonrecurring_effective_weight asc) AS row_num,  
                					percent_rank() OVER (partition by index_code ORDER BY pe_ttm_nonrecurring*100/pe_ttm_nonrecurring_effective_weight asc) AS percent_num
                					from aggregated_data.index_components_historical_estimations
                					where index_code = '%s'
                					and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                					left join 
                					(select index_code, count(1) as total_num from aggregated_data.index_components_historical_estimations where index_code = '%s' and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
                					on raw.index_code = record.index_code
                					where raw.latest_date = (select max(historical_date) from aggregated_data.index_components_historical_estimations)
                					order by raw.percent_num asc """ % (years, index_code, years, index_code, years)
        # 市净率
        elif (valuation_method == "pb"):
            selecting_sql = """  select raw.index_code, raw.index_name, raw.latest_date, round(raw.pb_effective, 4) as pb_effective, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as previous_year_num  from 
                					(select index_code, index_name, historical_date as latest_date, pb*100/pb_effective_weight as pb_effective,
                					row_number() OVER (partition by index_code ORDER BY pb*100/pb_effective_weight asc) AS row_num,  
                					percent_rank() OVER (partition by index_code ORDER BY pb*100/pb_effective_weight asc) AS percent_num
                					from aggregated_data.index_components_historical_estimations
                					where index_code = '%s'
                					and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                					left join 
                					(select index_code, count(1) as total_num from aggregated_data.index_components_historical_estimations where index_code = '%s' and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
                					on raw.index_code = record.index_code
                					where raw.latest_date = (select max(historical_date) from aggregated_data.index_components_historical_estimations)
                					order by raw.percent_num asc """ % (years, index_code, years, index_code, years)
        # 扣商誉市净率
        elif (valuation_method == "pb_wo_gw"):
            selecting_sql = """  select raw.index_code, raw.index_name, raw.latest_date, round(raw.pb_wo_gw_effective, 4) as pb_wo_gw_effective, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as previous_year_num  from 
                					(select index_code, index_name, historical_date as latest_date, pb_wo_gw*100/pb_wo_gw_effective_weight as pb_wo_gw_effective,
                					row_number() OVER (partition by index_code ORDER BY pb_wo_gw*100/pb_wo_gw_effective_weight asc) AS row_num,  
                					percent_rank() OVER (partition by index_code ORDER BY pb_wo_gw*100/pb_wo_gw_effective_weight asc) AS percent_num
                					from aggregated_data.index_components_historical_estimations
                					where index_code = '%s'
                					and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                					left join 
                					(select index_code, count(1) as total_num from aggregated_data.index_components_historical_estimations where index_code = '%s' and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
                					on raw.index_code = record.index_code
                					where raw.latest_date = (select max(historical_date) from aggregated_data.index_components_historical_estimations)
                					order by raw.percent_num asc """ % (years, index_code, years, index_code, years)
        # 滚动市销率
        elif (valuation_method == "ps_ttm"):
            selecting_sql = """  select raw.index_code, raw.index_name, raw.latest_date, round(raw.ps_ttm_effective, 4) as ps_ttm_effective, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as previous_year_num  from 
                					(select index_code, index_name, historical_date as latest_date, ps_ttm*100/ps_ttm_effective_weight as ps_ttm_effective,
                					row_number() OVER (partition by index_code ORDER BY ps_ttm*100/ps_ttm_effective_weight asc) AS row_num,  
                					percent_rank() OVER (partition by index_code ORDER BY ps_ttm*100/ps_ttm_effective_weight asc) AS percent_num
                					from aggregated_data.index_components_historical_estimations
                					where index_code = '%s'
                					and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                					left join 
                					(select index_code, count(1) as total_num from aggregated_data.index_components_historical_estimations where index_code = '%s' and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
                					on raw.index_code = record.index_code
                					where raw.latest_date = (select max(historical_date) from aggregated_data.index_components_historical_estimations)
                					order by raw.percent_num asc """ % (years, index_code, years, index_code, years)

        # 滚动市销率
        elif (valuation_method == "pcf_ttm"):
            selecting_sql = """  select raw.index_code, raw.index_name, raw.latest_date, round(raw.pcf_ttm_effective, 4) as pcf_ttm_effective, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as previous_year_num  from 
                					(select index_code, index_name, historical_date as latest_date, pcf_ttm*100/pcf_ttm_effective_weight as pcf_ttm_effective,
                					row_number() OVER (partition by index_code ORDER BY pcf_ttm*100/pcf_ttm_effective_weight asc) AS row_num,  
                					percent_rank() OVER (partition by index_code ORDER BY pcf_ttm*100/pcf_ttm_effective_weight asc) AS percent_num
                					from aggregated_data.index_components_historical_estimations
                					where index_code = '%s'
                					and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                					left join 
                					(select index_code, count(1) as total_num from aggregated_data.index_components_historical_estimations where index_code = '%s' and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
                					on raw.index_code = record.index_code
                					where raw.latest_date = (select max(historical_date) from aggregated_data.index_components_historical_estimations)
                					order by raw.percent_num asc """ % (years, index_code, years, index_code, years)

        # 股息率
        elif (valuation_method == "dividend_yield"):
            selecting_sql = """  select raw.index_code, raw.index_name, raw.latest_date, round(raw.dividend_yield_effective, 4) as dividend_yield_effective, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as previous_year_num  from 
                					(select index_code, index_name, historical_date as latest_date, dividend_yield*100/dividend_yield_effective_weight as dividend_yield_effective,
                					row_number() OVER (partition by index_code ORDER BY dividend_yield*100/dividend_yield_effective_weight asc) AS row_num,  
                					percent_rank() OVER (partition by index_code ORDER BY dividend_yield*100/dividend_yield_effective_weight asc) AS percent_num
                					from aggregated_data.index_components_historical_estimations
                					where index_code = '%s'
                					and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                					left join 
                					(select index_code, count(1) as total_num from aggregated_data.index_components_historical_estimations where index_code = '%s' and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
                					on raw.index_code = record.index_code
                					where raw.latest_date = (select max(historical_date) from aggregated_data.index_components_historical_estimations)
                					order by raw.percent_num asc """ % (years, index_code, years, index_code, years)
        # 其它
        else:
            return None
        index_estiamtion_info = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        return index_estiamtion_info

if __name__ == '__main__':
    time_start = time.time()
    go = IndexComponentsHistoricalEstimationMapper()
    # result = go.get_index_historical_date_estimation("399997", "pe_ttm", "2023-05-12")
    # result = go.get_index_a_period_estimation("399997", "pe_ttm", "2023-05-12", 5)
    #result = go.get_last_trading_date_pe_ttm_nonrecurring("399997", "2023-05-30", "2023-05-12")
    result = go.get_last_trading_date_pb_wo_gw("399986", "2023-06-03", "2023-06-02")
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)
