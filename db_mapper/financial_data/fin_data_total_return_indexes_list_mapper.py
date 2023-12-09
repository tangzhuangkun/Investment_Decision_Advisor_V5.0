#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import sys

sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger

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

    def insert_tr_total_return_data(self, index_code, index_name, index_name_init, issuer, source, index_code_tr,
                                    index_name_tr, index_name_init_tr, issuer_tr, source_tr):
        # 插入的SQL
        insert_sql = ((
                          "INSERT IGNORE INTO fin_data_total_return_indexes_list (index_code, index_name, index_name_init, issuer, `source`, "
                          "index_code_tr, index_name_tr, index_name_init_tr, issuer_tr, source_tr)  "
                          "VALUES ('%s','%s','%s','%s','%s','%s', '%s','%s','%s','%s' )") %
                      (index_code, index_name, index_name_init, issuer, source, index_code_tr, index_name_tr,
                       index_name_init_tr, issuer_tr, source_tr))
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
        select_sql = (
                         " select index_code, index_name, index_code_tr, index_name_tr from fin_data_total_return_indexes_list "
                         "where source = '%s' ") % (source)
        db_content = db_operator.DBOperator().select_all("financial_data", select_sql)
        return db_content

    """
    检查有哪些关联错误或者重复的全收益指数
    """
    def check_duplicated_tr_index(self):
        select_sql = """ select b.index_code, b.index_name, b.index_code_tr, b.index_name_tr
                            from 
                            (
                            -- 哪些常规指数关联了多条
                            select distinct index_code, index_name, count(index_code) as cnt
                            from fin_data_total_return_indexes_list
                            group by index_code, index_name
                            having count(index_code) > 1) as a
                            left join 
                            (select index_code, index_name, index_code_tr, index_name_tr
                            from fin_data_total_return_indexes_list) as b
                            on a.index_code = b.index_code
                            order by a.cnt desc, index_code
                    """
        db_content = db_operator.DBOperator().select_all("financial_data", select_sql)
        return db_content

    """
    删除关联错误或者重复的全收益指数, 
    重点保留人民币计价的全收益指数
    """

    def delete_duplicated_tr_index(self):
        delete_sql = """
                        -- 中证香港50指数(人民币)
                        delete from fin_data_total_return_indexes_list where index_code = 'H30342' and index_code_tr = 'H20420';
                        delete from fin_data_total_return_indexes_list where index_code = 'H30342' and index_code_tr = 'H20422';
                        delete from fin_data_total_return_indexes_list where index_code = 'H30342' and index_code_tr = 'H20436';
                        delete from fin_data_total_return_indexes_list where index_code = 'H30342' and index_code_tr = 'H20438';
                        
                        -- 中证香港100指数(人民币)
                        delete from fin_data_total_return_indexes_list where index_code = 'H11101' and index_code_tr = 'H20418';
                        delete from fin_data_total_return_indexes_list where index_code = 'H11101' and index_code_tr = 'H20421';
                        delete from fin_data_total_return_indexes_list where index_code = 'H11101' and index_code_tr = 'H20437';
                        
                        -- 中证香港200指数(人民币)
                        delete from fin_data_total_return_indexes_list where index_code = 'H11169' and index_code_tr = 'H20419';
                        delete from fin_data_total_return_indexes_list where index_code = 'H11169' and index_code_tr = 'H20435';
                        
                        -- 沪深300红利指数
                        delete from fin_data_total_return_indexes_list where index_code = '000821' and index_code_tr = 'H20740';
                        
                        -- 中证香港银行投资指数
                        delete from fin_data_total_return_indexes_list where index_code = '000869' and index_code_tr = 'H20792';
                        
                        -- 中证香港证券投资主题指数
                        delete from fin_data_total_return_indexes_list where index_code = '930709' and index_code_tr = 'H20709';
                        
                        -- 中证香港中小企业投资主题指数
                        delete from fin_data_total_return_indexes_list where index_code = '930746' and index_code_tr = 'H20746';
                        delete from fin_data_total_return_indexes_list where index_code = '000867' and index_code_tr = 'H20746';
                        
                        -- 中证香港红利等权投资指数
                        delete from fin_data_total_return_indexes_list where index_code = '930784' and index_code_tr = 'H20784';
                        
                        -- 中证香港银行投资指数
                        delete from fin_data_total_return_indexes_list where index_code = '930792' and index_code_tr = 'H20792';
                        
                        -- 中证新能源汽车产业指数
                        delete from fin_data_total_return_indexes_list where index_code = '930997' and index_code_tr = 'H20522';
                        
                        -- 中证科技50指数
                        delete from fin_data_total_return_indexes_list where index_code = '931380' and index_code_tr = '921520';
                        
                        -- 中证香港上市可交易香港地产指数
                        delete from fin_data_total_return_indexes_list where index_code = 'H11142' and index_code_tr = 'H01142';
                        
                        -- 中证香港上市可交易内地银行指数
                        delete from fin_data_total_return_indexes_list where index_code = 'H11145' and index_code_tr = 'H01145';
                                             
                        -- 中证香港上市可交易内地金融指数
                        delete from fin_data_total_return_indexes_list where index_code = 'H11146' and index_code_tr = 'H01146';
                        
                        -- 中证金砖国家60DR指数
                        delete from fin_data_total_return_indexes_list where index_code = 'H30133' and index_code_tr = 'H20135';
                        
                        -- 中证红利低波动指数
                        delete from fin_data_total_return_indexes_list where index_code = 'H30269' and index_code_tr = 'H20955';
                        
                        -- 中证500成长指数
                        delete from fin_data_total_return_indexes_list where index_code = 'H30351' and index_code_tr = 'H20938';
                        
                        -- 中证香港100动量指数
                        delete from fin_data_total_return_indexes_list where index_code = 'H30418' and index_code_tr = 'H20418';
                        
                        -- 中证香港200动量指数
                        delete from fin_data_total_return_indexes_list where index_code = 'H30419' and index_code_tr = 'H20419';
                        
                        -- 中证香港50等风险贡献指数
                        delete from fin_data_total_return_indexes_list where index_code = 'H30420' and index_code_tr = 'H20420';
                        
                        -- 中证香港100等风险贡献指数
                        delete from fin_data_total_return_indexes_list where index_code = 'H30421' and index_code_tr = 'H20421';
                        
                        -- 中证香港50等权重指数
                        delete from fin_data_total_return_indexes_list where index_code = 'H30422' and index_code_tr = 'H20422';
                     """
        result = db_operator.DBOperator().operate("delete", "financial_data", delete_sql)

        if result.get('status') == True:
            # 日志记录
            msg = " 删除关联错误或者重复的全收益指数, 成功"
            custom_logger.CustomLogger().log_writter(msg, lev='info')
        else:
            # 日志记录
            msg = "删除关联错误或者重复的全收益指数, 失败, " + result.get('msg')
            custom_logger.CustomLogger().log_writter(msg)
