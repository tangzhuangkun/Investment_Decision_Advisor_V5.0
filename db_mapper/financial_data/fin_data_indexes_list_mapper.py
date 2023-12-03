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
        insert_sql = ("INSERT IGNORE INTO fin_data_indexes_list(index_code, index_name, index_name_init, securities_num, issuer, source) "
                         "VALUES ('%s','%s','%s','%s','%s','%s')") % ( index_code, index_name, index_name_init, securities_num, issuer, source)
        db_operator.DBOperator().operate("insert", "financial_data", insert_sql)


    """
    根据指数属性查询指数
    :param, source， 数据源
    :param, containOrNot， 是否包含
    :param, index_name_feature， 指数名属性
    :return 
    index_code	index_name	index_name_init	issuer	source
    000001	上海证券交易所综合指数	上证指数	XSHG	米筐
    000002	上海证券交易所Ａ股指数	A股指数	XSHG	米筐
    000003	上海证券交易所Ｂ股指数	B股指数	XSHG	米筐
    000004	上海证券交易所工业指数	工业指数	XSHG	米筐
    """
    def select_index_name_feature(self, source, containOrNot, index_name_feature):
        select_sql = (" select index_code, index_name, index_name_init, issuer, source from fin_data_indexes_list "
                      "where source = '%s' and index_name %s like '%%s%' ") % ( source, containOrNot, index_name_feature)
        db_content = db_operator.DBOperator().select_all("financial_data", select_sql)
        return db_content


    """
    根据指数属性查询相似全收益指数
    :param, source， 数据源
    :param, compare_len， 对比的指数名称字符长度
    :param, compare_len_init， 对比的指数缩写名称字符长度
    :return
    index_code	index_name	index_name_init	issuer	index_code_tr	index_name_tr	index_name_init_tr	issuer_tr
    000050	上证50等权重指数	50等权	XSHG	H00050	上证50等权重全收益指数	50等权全收益	INDX
    000052	上证50基本面加权指数	50基本	XSHG	H00052	上证50基本面加权全收益指数	50基本全收益	INDX
    000053	上证180基本面加权指数	180基本	XSHG	H00053	上证180基本面加权全收益指数	180基本全收益	INDX
    000054	上证海外上市A股指数	上证海外	XSHG	H00054	上证海外上市A股全收益指数	上证海外全收益	INDX
    000055	上证地方国有企业50指数	上证地企	XSHG	H00055	上证地方国有企业50全收益指数	上证地企全收益	INDX
    000056	上证国有企业100指数	上证国企	XSHG	H00056	上证国有企业100全收益指数	上证国企全收益	INDX
    """
    def select_similar_total_return_index(self, source, compare_len, compare_len_init):
        select_sql = ("select t1.index_code, t1.index_name, t1.index_name_init, t1.issuer, "
                      "t2.index_code as index_code_tr, t2.index_name as index_name_tr, t2.index_name_init as index_name_init_tr, "
                      "t2.issuer as issuer_tr from (select index_code, index_name, index_name_init, issuer, "
                      "source from fin_data_indexes_list where source = '%s' and index_name not like '%%全收益%%' "
                      "and index_name not like '%%美元%%' and index_name not like '%%港币%%' "
                      "and index_name not like '%%港元%%' and index_name not like '%%台币%%' "
                      "and index_name not like '中信证券%%' and index_name not like '申银万国指数%%') as t1 "
                      "left join "
                      "(select index_code, index_name, index_name_init, issuer, source from fin_data_indexes_list "
                      "where source = '%s' and index_name not like '%%美元%%' and index_name not like '%%港币%%' "
                      "and index_name not like '%%港元%%' and index_name not like '%%台币%%' and index_name like '%%全收益%%' ) as t2 "
                      "on t1.source = t2.source where t1.index_code != t2.index_code "
                      "and CHAR_LENGTH(t1.index_name) <= CHAR_LENGTH(t2.index_name) "
                      "and left(t1.index_name, %s) = left(t2.index_name, %s) "
                      "and left(t1.index_name_init, %s) = left(t2.index_name_init, %s) "
                      "and t2.index_name_init like concat('%%', t1.index_name_init, '%%') "
                      "order by t1.index_code") % (source, source, compare_len, compare_len, compare_len_init, compare_len_init)
        db_content = db_operator.DBOperator().select_all("financial_data", select_sql)
        return db_content


    """
    删除数据库中某特定数据源的记录
    :param, source， 数据源
    """
    def delete_specific_source_record(self, source):
        delete_sql = ("delete from fin_data_indexes_list where source = '%s' ") % (source)
        db_operator.DBOperator().operate("delete", "financial_data", delete_sql)






