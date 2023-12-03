#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun



import sys
sys.path.append("..")
import database.db_operator as db_operator

"""
数据表，fin_data_total_return_indexes_list 的映射
以该表为主的数据操作，均在此完成
"""


class FinDataTotalReturnIndexesListMapper:

    def __init__(self):
        pass

    """
    将列表信息存入数据库
    """
    def insert_tr_total_return_data(self, index_code, index_name, index_name_init, issuer, source, index_code_tr, index_name_tr, index_name_init_tr, issuer_tr, source_tr):
        # 插入的SQL
        insert_sql = (("INSERT IGNORE INTO fin_data_total_return_indexes_list (index_code, index_name, index_name_init, issuer, `source`, "
                      "index_code_tr, index_name_tr, index_name_init_tr, issuer_tr, source_tr)  "
                      "VALUES ('%s','%s','%s','%s','%s','%s', '%s','%s','%s','%s' )") %
                      ( index_code, index_name, index_name_init, issuer, source, index_code_tr, index_name_tr, index_name_init_tr, issuer_tr, source_tr))
        db_operator.DBOperator().operate("insert", "financial_data", insert_sql)


    """
    删除数据库中某特定数据源的记录
    :param, source， 数据源
    """
    def delete_specific_source_record(self, source):
        delete_sql = ("delete from fin_data_total_return_indexes_list where source = '%s' ") % (source)
        db_operator.DBOperator().operate("delete", "financial_data", delete_sql)


    """
    获取特定数据源的指数及其相关全收益指数
    :param, source， 数据源， 默认 米筐
    """
    def select_index_name_feature(self, source="米筐"):
        select_sql = (" select index_code, index_name, index_code_tr, index_name_tr from fin_data_total_return_indexes_list "
                      "where source = '%s' ") % ( source)
        db_content = db_operator.DBOperator().select_all("financial_data", select_sql)
        return db_content