#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import time

import sys
sys.path.append("..")
import database.db_operator as db_operator

"""
数据表，fin_data_indexes_list 的映射
以该表为主的数据操作，均在此完成
"""
class FinDataIndexesListMapper:

    def __init__(self):
        pass

    """
    将指数列表信息存入数据库
    :param, index_code, 指数代码
    :param, index_name, 指数名称
    :param, securities_num, 成分股个数
    :param, issuer, 指数开发公司
    :param, source, 数据来源
    """

    def insert_all_indexes(self, index_code, index_name, issuer, source, securities_num=None, index_name_init=None):
        # 插入的SQL
        inserting_sql = ("INSERT IGNORE INTO fin_data_indexes_list(index_code, index_name, index_name_init, securities_num, issuer, source) "
                         "VALUES ('%s','%s','%s','%s','%s','%s')") % ( index_code, index_name, index_name_init, securities_num, issuer, source)
        db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)