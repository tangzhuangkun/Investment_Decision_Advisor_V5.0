#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import time
import difflib

import sys
sys.path.append("..")
import log.custom_logger as custom_logger
import db_mapper.financial_data.fin_data_indexes_list_mapper as fin_data_indexes_list_mapper
import db_mapper.financial_data.fin_data_total_return_indexes_list_mapper as fin_data_total_return_indexes_list_mapper

class CalculateIndexSimilarTotalReturnIndexes:
    # 根据指数名称相似度，以求挖掘指数相关的全收益指数
    # 运行频率：指定运行

    def __init__(self):

        # 对比的指数名称字符长度
        self.compare_len = 6
        # 对比的指数缩写名称字符长度
        self.compare_len_init = 4
        # 数据源
        self.data_source = "米筐"


    """
    Jaccard相似度通过计算两个集合之间的交集和并集之间的比率来衡量相似性， Jaccard系数值越大，样本相似度越高, 相似度最大为1
    :param, text1, 字符串
    :param, text2, 字符串
    """
    def calculate_jaccard_similarity(self, text1, text2):
        set1 = set(text1.split())
        set2 = set(text2.split())
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        return intersection / union

    """
    python自带比较相似度的模块，difflib。比较两个字符串的模块是difflib.SequenceMatcher, 系数值越大，样本相似度越高, 相似度最大为1
    其中None的位置是一个函数，用来去掉自己不想算在内的元素
    :param, text1, 字符串
    :param, text2, 字符串
    """
    def string_similar(self, text1, text2):
        return difflib.SequenceMatcher(None, text1, text2).quick_ratio()


    """
    计算挖掘并储存 指数相关的全收益指数
    :param, source， 数据源
    """
    def cal_and_save_given_source_index_name_similarity(self):

        # 存入数据之前，先清除数据库的数据记录
        fin_data_total_return_indexes_list_mapper.FinDataTotalReturnIndexesListMapper().delete_specific_source_record(self.data_source)

        # 获取相似指数名称列表
        # 如 [{'index_code': '000050', 'index_name': '上证50等权重指数', 'index_name_init': '50等权', 'issuer': 'XSHG', 'index_code_tr': 'H00050', 'index_name_tr': '上证50等权重全收益指数', 'index_name_init_tr': '50等权全收益', 'issuer_tr': 'INDX'},
        # {'index_code': '000052', 'index_name': '上证50基本面加权指数', 'index_name_init': '50基本', 'issuer': 'XSHG', 'index_code_tr': 'H00052', 'index_name_tr': '上证50基本面加权全收益指数', 'index_name_init_tr': '50基本全收益', 'issuer_tr': 'INDX'},,,,]
        similar_index_list = fin_data_indexes_list_mapper.FinDataIndexesListMapper().select_similar_total_return_index(self.data_source, self.compare_len, self.compare_len_init)
        for unit in similar_index_list:
            # 指数代码
            index_code = unit["index_code"]
            # 指数名称
            index_name =  unit['index_name']
            # 指数简称
            index_name_init = unit['index_name_init']
            # 发行人
            issuer = unit['issuer']
            # 数据源
            source = self.data_source
            # 相似相关指数代码
            index_code_tr = unit['index_code_tr']
            # 相似相关指数名称
            index_name_tr = unit['index_name_tr']
            # 相似相关指数名称简称
            index_name_init_tr  = unit['index_name_init_tr']
            # 相似相关指数发行人
            issuer_tr = unit['issuer_tr']
            source_tr = self.data_source
            # 存入数据库
            fin_data_total_return_indexes_list_mapper.FinDataTotalReturnIndexesListMapper().insert_tr_total_return_data(index_code, index_name, index_name_init, issuer, source, index_code_tr, index_name_tr, index_name_init_tr, issuer_tr, source_tr)

        # 日志记录
        msg = " 计算挖掘并储存 指数相关的全收益指数"
        custom_logger.CustomLogger().log_writter(msg, lev='info')


    """
    删除关联错误或者重复的全收益指数, 
    重点保留人民币计价的全收益指数
    """
    def delete_duplicated_indexes(self):
        fin_data_total_return_indexes_list_mapper.FinDataTotalReturnIndexesListMapper().delete_duplicated_tr_index()


    def main(self):
        self.cal_and_save_given_source_index_name_similarity()
        self.delete_duplicated_indexes()

if __name__ == '__main__':
    time_start = time.time()
    go = CalculateIndexSimilarTotalReturnIndexes()
    go.main()
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))