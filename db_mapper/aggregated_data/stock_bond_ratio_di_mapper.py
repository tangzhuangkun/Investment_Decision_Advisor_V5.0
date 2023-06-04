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
                            from stock_bond_ratio_di 
                            where index_code = '%s' and trading_date = '%s' """ % (index_code, p_day)
        # 查询
        selecting_result = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        return selecting_result





if __name__ == '__main__':
    time_start = time.time()
    go = StockBondRatioDiMapper()
    result = go.get_a_specific_day_stock_bond_ratio_info("000300", "2023-06-02")
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)