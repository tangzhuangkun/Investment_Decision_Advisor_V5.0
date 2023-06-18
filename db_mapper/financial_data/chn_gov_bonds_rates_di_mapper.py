#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import time

import sys

sys.path.append("..")
import database.db_operator as db_operator


"""
数据表，chn_gov_bonds_rates_di 的映射
以该表为主的数据操作，均在此完成
"""

class ChnGovBondsRatesDiMapper:

    def __init__(self):
        pass


    """
    收集债券利率信息
    :param, period, 期限，如 1m --1月，2m --2月，3m --3月，6m --6月，9m --9月，1y --1年，2y --2年，3y --3年，5y --5年，7y --7年，10y --10年
    :param, rate, 债券利率
    :param, p_day, 业务日期
    :param, source, 数据来源
    :param, submission_date, 提交日期
    """
    def collect_bond_rate(self, period, rate, p_day, source, submission_date):
        # 插入的SQL
        inserting_sql = """INSERT INTO financial_data.chn_gov_bonds_rates_di (%s,trading_day,source,submission_date) 
                            VALUES ('%s','%s','%s','%s')""" % (period, rate, p_day, source, submission_date)
        db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)

    """
    统计总行数
    """
    def count_rows(self):
        # 查询sql
        selecting_sql = """SELECT COUNT(*) as total_rows FROM financial_data.chn_gov_bonds_rates_di"""
        # 查询
        result = db_operator.DBOperator().select_one("financial_data", selecting_sql)
        return result

    """
    最新的日期
    """
    def max_date(self):
        # 查询sql
        selecting_max_date_sql = """SELECT max(trading_day) as max_day FROM financial_data.chn_gov_bonds_rates_di"""
        # 查询
        max_date = db_operator.DBOperator().select_one("financial_data", selecting_max_date_sql)
        return max_date

    """
    更新债券信息
    :param, period, 期限，如 1m --1月，2m --2月，3m --3月，6m --6月，9m --9月，1y --1年，2y --2年，3y --3年，5y --5年，7y --7年，10y --10年
    :param, rate, 债券利率
    :param, p_day, 业务日期
    :param, source, 数据来源
    """
    def update_bond_info(self, period, rate, p_day, source):

        updating_sql = "UPDATE chn_gov_bonds_rates_di SET %s  = '%s' WHERE trading_day = '%s' AND source = '%s' " % (period, rate, p_day, source)
        db_operator.DBOperator().operate("update", "financial_data", updating_sql)