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
    :param, one_year_volatility, 近1年年化波动率，如 12.73
    :param, three_year_volatility, 3年年化波动率，如 12.73
    :param, five_year_volatility, 5年年化波动率，如 12.73
    :param, relative_fund_code, 相关指数基金代码，如 161725
    :param, relative_fund_name, 相关指数基金名称，如 招商中证白酒指数证券投资基金
    :param, p_day
    """
    def insert_regular_excellent_indexes(self, index_code, index_name, index_company, one_month_yield_rate, three_month_yield_rate, this_year_yield_rate, one_year_yield_rate, three_year_yield_rate, five_year_yield_rate, one_year_volatility, three_year_volatility, five_year_volatility, relative_fund_code, relative_fund_name, p_day):
        # 插入的SQL
        inserting_sql = "INSERT IGNORE INTO index_excellent_performance_indices_di(index_code, index_name, index_company, one_month_yield_rate, three_month_yield_rate, this_year_yield_rate, one_year_yield_rate, three_year_yield_rate,five_year_yield_rate, one_year_volatility, three_year_volatility, five_year_volatility, relative_fund_code, relative_fund_name,p_day)" \
                        "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                            index_code, index_name, index_company, one_month_yield_rate, three_month_yield_rate, this_year_yield_rate, one_year_yield_rate, three_year_yield_rate, five_year_yield_rate,
                            one_year_volatility, three_year_volatility, five_year_volatility, relative_fund_code, relative_fund_name, p_day)
        # 将python中的空值替换为mysql中的空值
        inserting_sql = inserting_sql.replace(", 'None'", ', NULL')
        db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)


    """
    将优秀的指数, 全收益指数， 净收益指数 及其相关基金存入数据库
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
    :param, index_code_net, 净收益指数代码',
	:param, index_name_net, 净收益指数名称',
    :param, one_month_yield_rate_net, 净收益近1月年化收益率', 如 12.73
	:param, three_month_yield_rate_net, 净收益近3月年化收益率', 如 12.73
    :param, this_year_yield_rate_net, 净收益年至今年化收益率', 如 12.73
	:param, one_year_yield_rate_net, 净收益近1年年化收益率', 如 12.73
    :param, three_year_yield_rate_net, 净收益近3年年化收益率', 如 12.73
	:param, five_year_yield_rate_net, 净收益近5年年化收益率', 如 12.73
	:param, one_year_volatility, 近1年年化波动率，如 12.73
    :param, three_year_volatility, 3年年化波动率，如 12.73
    :param, five_year_volatility, 5年年化波动率，如 12.73
	:param, relative_fund_code, 相关指数基金代码，如 161725
    :param, relative_fund_name, 相关指数基金名称，如 招商中证白酒指数证券投资基金
    :param, p_day
    """
    def insert_excellent_indexes_overall_info(self, index_code, index_name, index_company,
                                                          one_month_yield_rate, three_month_yield_rate, this_year_yield_rate, one_year_yield_rate, three_year_yield_rate, five_year_yield_rate,
                                                          index_code_tr, index_name_tr, one_month_yield_rate_tr, three_month_yield_rate_tr, this_year_yield_rate_tr, one_year_yield_rate_tr, three_year_yield_rate_tr, five_year_yield_rate_tr,
                                                          index_code_net, index_name_net, one_month_yield_rate_net,
                                                          three_month_yield_rate_net, this_year_yield_rate_net,
                                                          one_year_yield_rate_net, three_year_yield_rate_net,
                                                          five_year_yield_rate_net,
                                                          one_year_volatility, three_year_volatility, five_year_volatility,
                                                          relative_fund_code, relative_fund_name, p_day):
        # # 如果全收益代码为空，则将全收益所有信息置为空或0
        # if index_code_tr == None:
        #     index_code_tr = None
        #     index_name_tr = None
        #     one_month_yield_rate_tr = 0
        #     three_month_yield_rate_tr =  0
        #     this_year_yield_rate_tr =  0
        #     one_year_yield_rate_tr =  0
        #     three_year_yield_rate_tr =  0
        #     five_year_yield_rate_tr =  0
        #
        # # 如果净收益代码为空，则将全收益所有信息置为空或0
        # if index_code_net == None:
        #     index_code_net = None
        #     index_name_net = None
        #     one_month_yield_rate_net =  0
        #     three_month_yield_rate_net =  0
        #     this_year_yield_rate_net = 0
        #     one_year_yield_rate_net =  0
        #     three_year_yield_rate_net =  0
        #     five_year_yield_rate_net =  0

        # 插入的SQL
        inserting_sql = "INSERT IGNORE INTO index_excellent_performance_indices_di(index_code, index_name, index_company, one_month_yield_rate, three_month_yield_rate, this_year_yield_rate, one_year_yield_rate, three_year_yield_rate,five_year_yield_rate, index_code_tr, index_name_tr, one_month_yield_rate_tr, three_month_yield_rate_tr, this_year_yield_rate_tr, one_year_yield_rate_tr, three_year_yield_rate_tr, five_year_yield_rate_tr, index_code_net, index_name_net, one_month_yield_rate_net, three_month_yield_rate_net, this_year_yield_rate_net, one_year_yield_rate_net, three_year_yield_rate_net, five_year_yield_rate_net, one_year_volatility, three_year_volatility, five_year_volatility, relative_fund_code, relative_fund_name, p_day)" \
                        " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (
                            index_code, index_name, index_company, one_month_yield_rate, three_month_yield_rate, this_year_yield_rate, one_year_yield_rate, three_year_yield_rate, five_year_yield_rate,
                            index_code_tr, index_name_tr, one_month_yield_rate_tr, three_month_yield_rate_tr, this_year_yield_rate_tr, one_year_yield_rate_tr, three_year_yield_rate_tr, five_year_yield_rate_tr, index_code_net, index_name_net, one_month_yield_rate_net, three_month_yield_rate_net, this_year_yield_rate_net, one_year_yield_rate_net, three_year_yield_rate_net, five_year_yield_rate_net,one_year_volatility, three_year_volatility, five_year_volatility, relative_fund_code, relative_fund_name, p_day)
        # 将python中的空值替换为mysql中的空值
        inserting_sql= inserting_sql.replace(", 'None'", ', NULL')
        db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)