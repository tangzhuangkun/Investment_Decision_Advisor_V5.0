#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import time

import sys
sys.path.append("..")
import database.db_operator as db_operator

"""
数据表，index_excellent_performance_indices_di 的映射
以该表为主的数据操作，均在此完成
"""
class IndexExcellentPerformanceIndicesDiMapper:

    def __init__(self):
        pass

    """
    将优秀的指数及其相关基金存入数据库
    :param, index_code, 指数代码，如 399997
    :param, index_name, 指数名称，如 中证白酒
    :param, index_company, 指数开发公司，如 中证
    :param, three_year_yield_rate, 3年年化收益率，如 12.73
    :param, five_year_yield_rate, 5年年化收益率，如 12.73
    :param, relative_fund_code, 相关指数基金代码，如 161725
    :param, relative_fund_name, 相关指数基金名称，如 招商中证白酒指数证券投资基金
    :param, p_day
    """
    def insert_excellent_indexes(self, index_code, index_name, index_company, three_year_yield_rate, five_year_yield_rate, relative_fund_code, relative_fund_name, p_day):
        # 插入的SQL
        inserting_sql = "INSERT INTO index_excellent_performance_indices_di(index_code,index_name," \
                        "index_company,three_year_yield_rate,five_year_yield_rate,relative_fund_code," \
                        "relative_fund_name,p_day)" \
                        "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                            index_code, index_name, index_company, three_year_yield_rate, five_year_yield_rate,
                            relative_fund_code, relative_fund_name, p_day)
        db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)