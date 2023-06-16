#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import time
import sys

sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger


class StockBondRatioDiMapper:

    def __init__(self):
        pass


    """
    获取某个交易日，某指数与国债的收益率信息
    :param index_code, 指数代码，如 000300
    :param p_day， 某个交易日， 如 2023-06-02
    :return, 指数代码，指数名称，交易日，指数当日涨跌幅，指数市盈率，指数收益率(市盈率倒数)，10年期债券收益率，股债收益率
    {'index_code': '000300', 'index_name': '沪深300', 'p_day': datetime.date(2023, 6, 2), 
    'index_change_rate': Decimal('0.01443705721498239800'), 'index_pe': Decimal('11.84731402813075200'), 
    'stock_yield_rate': Decimal('0.084407'), '10y_bond_rate': Decimal('0.026951'), 
    'ratio': Decimal('3.13186894734889243')}
    """
    def get_a_specific_day_stock_bond_ratio_info(self, index_code, p_day):
        selecting_sql = """select index_code, index_name, trading_date as p_day, index_cpc as index_change_rate, 
                            pe as index_pe, stock_yield_rate, 10y_bond_rate, ratio 
                            from aggregated_data.stock_bond_ratio_di 
                            where index_code = '%s' and trading_date = '%s' """ % (index_code, p_day)
        # 查询
        selecting_result = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        return selecting_result


    """
    获取某个交易日，某指数与国债的收益率信息,在过去X年的历史百分位
    :param index_code, 指数代码，如 000300
    :param p_day， 某个交易日， 如 2023-06-02
    :param year_num， 过去X年，如 3
    :return,  指数代码，指数名称，交易日，指数当日涨跌幅，指数市盈率，指数收益率(市盈率倒数)，10年期债券收益率，股债收益率, 所处X年历史百分位
    {'index_code': '000300', 'index_name': '沪深300', 'p_day': datetime.date(2023, 6, 2), 'index_change_rate': Decimal('0.01443705721498239800'), 'index_pe': Decimal('11.84731402813075200'), 'stock_yield_rate': Decimal('0.084407'), '10y_bond_rate': Decimal('0.026951'), 'ratio': Decimal('3.13186894734889243'), 'percent': 0.8928571428571429}
    """
    def get_a_specific_date_stock_bond_ratio_percentile_in_history(self, index_code, p_day, year_num):
        selecting_sql = """select index_code, index_name, p_day, index_change_rate, index_pe, stock_yield_rate, 10y_bond_rate, ratio, percent
                            from 
                            (select index_code, index_name, trading_date as p_day, index_cpc as index_change_rate, pe as index_pe, stock_yield_rate, 10y_bond_rate, ratio,
                            percent_rank() OVER (partition by index_code ORDER BY ratio) AS percent
                            from aggregated_data.stock_bond_ratio_di
                            where index_code = '%s'
                            and trading_date > SUBDATE('%s',INTERVAL '%s' YEAR)) as original
                            where p_day = '%s' """ % (index_code, p_day, year_num, p_day)
        # 查询
        selecting_result = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        return selecting_result


    """
    清空已计算好的股债比信息表
    """
    def truncate_table(self):
        truncating_sql = 'truncate table aggregated_data.stock_bond_ratio_di'
        try:
            db_operator.DBOperator().operate("update", "aggregated_data", truncating_sql)

        except Exception as e:
            # 日志记录
            msg = '失败，无法清空 aggregated_data数据库中的stock_bond_ratio_di表' + '  ' + str(e)
            custom_logger.CustomLogger().log_writter(msg, 'error')


if __name__ == '__main__':
    time_start = time.time()
    go = StockBondRatioDiMapper()
    #result = go.get_a_specific_day_stock_bond_ratio_info("000300", "2023-06-02")
    result = go.get_a_specific_date_stock_bond_ratio_percentile_in_history("000300", "2023-06-02", 3)
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)