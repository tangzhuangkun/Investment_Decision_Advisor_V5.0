#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import time
import difflib

import sys
sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger
import db_mapper.financial_data.fin_data_indexes_list_mapper as fin_data_indexes_list_mapper
import vo.index_similarity_vo as index_similarity_vo


class CalculateIndexSimilarity:
    # 根据指数名称相似度，以求挖掘指数相关的全收益，净收益指数
    # 运行频率：指定运行

    def __init__(self):

        # 对比的指数名称字符长度
        self.compare_len = 6
        # 对比的指数缩写名称字符长度
        self.compare_len_init = 4


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
    计算指数名称的相似性
    :param, source， 数据源
    """
    def cal_given_source_index_name_similarity(self, source):

        sim_index_info_dict = dict()

        # 获取相似指数名称列表
        # 如 [{'index_code': 'H30475', 'index_name': '中证沪港深互联互通中小综合信息技术指数(人民币)', 'issuer': 'XSHG', 'sim_index_code': 'H30513', 'sim_index_name': '中证沪港深互联互通综合工业港元指数', 'sim_issuer': 'XSHG'},
        # {'index_code': 'H30475', 'index_name': '中证沪港深互联互通中小综合信息技术指数(人民币)', 'issuer': 'XSHG', 'sim_index_code': 'H30514', 'sim_index_name': '中证沪港深互联互通综合可选消费港元指数', 'sim_issuer': 'XSHG'},,,]
        similar_index_list = fin_data_indexes_list_mapper.FinDataIndexesListMapper().select_similar_index_name(source, self.compare_len, self.compare_len_init)
        print(similar_index_list)
        """
        for item in similar_index_list:
            # 本指数信息（含 指数代码，指数名称，发行方）
            index_info = index_similarity_vo.IndexSimilarityVo(item.get('index_code'), item.get('index_name'),
                                                               item.get('issuer'))
            index_name = item.get('index_name')

            # 使用Jaccard计算相似度
            #similarity = self.calculate_jaccard_similarity(item.get("index_name"), item.get("sim_index_name"))
            # 使用difflib计算相似度
            similarity = self.string_similar(item.get("index_name"), item.get("sim_index_name"))
            # 如果相似度介于某区间
            if (similarity>=0.7 and similarity<=1):
                # 对比指数信息（含 指数代码，指数名称，发行方）
                index_info_sim = index_similarity_vo.IndexSimilarityVo(item.get('sim_index_code'), item.get('sim_index_name'),
                                                                   item.get('sim_issuer'), similarity)

                if index_name not in sim_index_info_dict:
                    sim_index_info_list = list()
                    sim_index_info_list.append(vars(index_info_sim))
                    sim_index_info_dict[index_name] = sim_index_info_list
                else:
                    sim_index_info_list = sim_index_info_dict.get(index_name)
                    sim_index_info_list.append(vars(index_info_sim))
                    sim_index_info_dict[index_name] = sim_index_info_list

        return sim_index_info_dict
        
        """


if __name__ == '__main__':
    time_start = time.time()
    go = CalculateIndexSimilarity()
    go.cal_given_source_index_name_similarity("米筐")
    #sim_index_info_dict = go.cal_given_source_index_name_similarity("米筐")
    #print(sim_index_info_dict)
    #ratio = go.calculate_jaccard_similarity("中证红利低波动100全收益指数", "红利低波100全收益")
    #ratio = go.string_similar("中证红利低波动100全收益指数", "红利低波100全收益")
    #print(ratio)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))