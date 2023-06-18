#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import time

import sys

sys.path.append("..")
import database.db_operator as db_operator

"""
数据表，index_constituent_stocks_weight 的映射
以该表为主的数据操作，均在此完成
"""


class IndexConstituentStocksWeightMapper:

    def __init__(self):
        pass

    """
    获取某个渠道，某指数，最新的的构成股票
    :param, source， 来源，如 中证权重文件， 中证官网， 国证官网
    :param, index_code, 如 399997
    :return, 如
    [{'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '603369', 'stock_name': '今世缘', 'weight': Decimal('4.473297363252433000'), 'p_day': datetime.date(2023, 6, 16)}, 
    {'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '600809', 'stock_name': '山西汾酒', 'weight': Decimal('12.894086834225680000'), 'p_day': datetime.date(2023, 6, 16)}, ,,,,]
    """

    def get_db_index_company_index_latest_component_stocks(self, source, index_code):
        # 查询sql
        selecting_sql = """select index_code, index_name, stock_code, stock_name, weight, p_day 
        from  index_constituent_stocks_weight 
        where p_day = (select max(p_day) as max_day from  index_constituent_stocks_weight where index_code = '%s' and source = '%s') 
        and  index_code = '%s' and source = '%s' order by stock_code desc""" % (
            index_code, source, index_code, source)
        db_index_content = db_operator.DBOperator().select_all("financial_data", selecting_sql)
        return db_index_content

    """
    将指数的信息存入数据库
    :param,index_code, 指数代码，如，399965
    :param,index_name, 指数名称，如，中证800地产
    :param,stock_code, 股票代码，如， 600519
    :param,stock_name, 股票名称，如，
    :param,stock_exchange_location, 股票上市地  如，sh
    :param,stock_market_code, 股票上市地代码   如，XSHG
    :param,weight,  权重，如，4.117342398775084000
    :param,source,  来源，如 中证权重文件， 中证官网， 国证官网
    :param,index_company,  指数公司，如，中证，国证
    :param,p_day, 业务日期，如，2023-06-15
    """

    def save_index_info(self, index_code, index_name, stock_code, stock_name, stock_exchange_location,
                        stock_market_code, weight, source, index_company, p_day):
        # 插入的SQL
        inserting_sql = """INSERT INTO index_constituent_stocks_weight(index_code,index_name, stock_code,stock_name,stock_exchange_location,stock_market_code, weight,source,index_company,p_day) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')""" % (
            index_code, index_name, stock_code, stock_name, stock_exchange_location,
            stock_market_code, weight, source, index_company, p_day)
        db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)


if __name__ == '__main__':
    time_start = time.time()
    go = IndexConstituentStocksWeightMapper()
    result = go.get_db_index_company_index_latest_component_stocks('中证官网', '399997')
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)
