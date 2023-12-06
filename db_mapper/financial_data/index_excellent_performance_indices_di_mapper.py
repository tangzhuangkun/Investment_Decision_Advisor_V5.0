#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import decimal

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
    :param, one_month_yield_rate, 近1月年化收益率，如 12.73
    :param, three_month_yield_rate, 近3月年化收益率，如 12.73
    :param, this_year_yield_rate, 今年年化收益率，如 12.73
    :param, one_year_yield_rate, 近1年年化收益率，如 12.73
    :param, three_year_yield_rate, 3年年化收益率，如 12.73
    :param, five_year_yield_rate, 5年年化收益率，如 12.73
    :param, relative_fund_code, 相关指数基金代码，如 161725
    :param, relative_fund_name, 相关指数基金名称，如 招商中证白酒指数证券投资基金
    :param, p_day
    """
    def insert_regular_excellent_indexes(self, index_code, index_name, index_company, one_month_yield_rate, three_month_yield_rate, this_year_yield_rate, one_year_yield_rate, three_year_yield_rate, five_year_yield_rate, relative_fund_code, relative_fund_name, p_day):
        # 插入的SQL
        inserting_sql = "INSERT INTO index_excellent_performance_indices_di(index_code, index_name, index_company, one_month_yield_rate, three_month_yield_rate, this_year_yield_rate, one_year_yield_rate, three_year_yield_rate,five_year_yield_rate,relative_fund_code, relative_fund_name,p_day)" \
                        "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                            index_code, index_name, index_company, one_month_yield_rate, three_month_yield_rate, this_year_yield_rate, one_year_yield_rate, three_year_yield_rate, five_year_yield_rate,
                            relative_fund_code, relative_fund_name, p_day)
        db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)


    """
    将优秀的指数及其相关基金存入数据库
    :param, index_code, 指数代码，如 399997
    :param, index_name, 指数名称，如 中证白酒
    :param, index_company, 指数开发公司，如 中证
    :param, one_month_yield_rate, 近1月年化收益率，如 12.73
    :param, three_month_yield_rate, 近3月年化收益率，如 12.73
    :param, this_year_yield_rate, 今年年化收益率，如 12.73
    :param, one_year_yield_rate, 近1年年化收益率，如 12.73
    :param, three_year_yield_rate, 3年年化收益率，如 12.73
    :param, five_year_yield_rate, 5年年化收益率，如 12.73
    :param, index_code_tr, 全收益指数代码，如 399997
    :param, index_name_tr, 全收益指数名称，如 中证白酒
    :param, one_month_yield_rate_tr, 全收益近1月年化收益率，如 12.73
    :param, three_month_yield_rate_tr, 全收益近3月年化收益率，如 12.73
    :param, this_year_yield_rate_tr, 全收益今年年化收益率，如 12.73
    :param, one_year_yield_rate_tr, 全收益近1年年化收益率，如 12.73
    :param, three_year_yield_rate_tr, 全收益3年年化收益率，如 12.73
    :param, five_year_yield_rate_tr, 全收益5年年化收益率，如 12.73
    :param, relative_fund_code, 相关指数基金代码，如 161725
    :param, relative_fund_name, 相关指数基金名称，如 招商中证白酒指数证券投资基金
    :param, p_day
    """
    def insert_regular_and_total_return_excellent_indexes(self, index_code, index_name, index_company, one_month_yield_rate, three_month_yield_rate, this_year_yield_rate, one_year_yield_rate, three_year_yield_rate, five_year_yield_rate, index_code_tr, index_name_tr, one_month_yield_rate_tr, three_month_yield_rate_tr, this_year_yield_rate_tr, one_year_yield_rate_tr, three_year_yield_rate_tr, five_year_yield_rate_tr, relative_fund_code, relative_fund_name, p_day):
        # 插入的SQL
        inserting_sql = "INSERT INTO index_excellent_performance_indices_di(index_code, index_name, index_company, one_month_yield_rate, three_month_yield_rate, this_year_yield_rate, one_year_yield_rate, three_year_yield_rate,five_year_yield_rate, index_code_tr, index_name_tr, one_month_yield_rate_tr, three_month_yield_rate_tr, this_year_yield_rate_tr, one_year_yield_rate_tr, three_year_yield_rate_tr, five_year_yield_rate_tr,relative_fund_code, relative_fund_name, p_day)" \
                        "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                            index_code, index_name, index_company, one_month_yield_rate, three_month_yield_rate, this_year_yield_rate, one_year_yield_rate, three_year_yield_rate, five_year_yield_rate,
                            index_code_tr, index_name_tr, one_month_yield_rate_tr, three_month_yield_rate_tr, this_year_yield_rate_tr, one_year_yield_rate_tr, three_year_yield_rate_tr, five_year_yield_rate_tr,relative_fund_code, relative_fund_name, p_day)
        db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)