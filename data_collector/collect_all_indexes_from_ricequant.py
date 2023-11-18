#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun
import time
import requests
import json
from bs4 import BeautifulSoup

import sys
sys.path.append("..")
import db_mapper.financial_data.fin_data_indexes_list_mapper as fin_data_indexes_list_mapper


class CollectAllIndexesFromRiceQuant:
    # 从米筐网收集 中国所有的指数列表
    # 运行频率：指定运行

    def __init__(self):
        self.ricequant_url = "https://www.ricequant.com/doc/rqdata/python/indices-dictionary.html"
        self.data_source = "米筐"

    """
    爬取并解析米筐网的指数列表页面
    """
    def parse_page_info(self):
        # 页面信息指数列表
        page_info_list = list()
        response = requests.get(self.ricequant_url)
        response.encoding = "utf-8"
        soup = BeautifulSoup(response.text, 'html.parser')
        items_list = soup.select("table tbody tr")
        for item in items_list:
            item_dict = dict()
            item_info = item.find_all("td")

            index_code_and_issuer = item_info[0].text.split(".")
            index_code = index_code_and_issuer[0]
            issuer = index_code_and_issuer[1]
            # 获取指数代码
            item_dict["index_code"] = index_code
            # 获取指数发行人
            item_dict["issuer"] = issuer
            # 获取指数简称
            item_dict["index_name_init"] = item_info[1].text
            # 获取指数全称
            item_dict["index_name"] = item_info[2].text
            page_info_list.append(item_dict)
        return page_info_list

    """
    将页面中的指数信息存入数据库
    :param, page_info_list，页面中的指数列表信息， 如 [{'index_code': '000001', 'issuer': 'XSHG', 'index_name_init': '上证指数', 'index_name': '上海证券交易所综合指数'}, {'index_code': '000002', 'issuer': 'XSHG', 'index_name_init': 'A股指数', 'index_name': '上海证券交易所Ａ股指数'},,,,]
    """
    def save_info_into_db(self, page_info_list):
        for item in page_info_list:
            index_code = item.get('index_code')
            index_name = item.get('index_name')
            index_name_init = item.get('index_name_init')
            issuer = item.get('issuer')
            fin_data_indexes_list_mapper.FinDataIndexesListMapper().insert_all_indexes(index_code, index_name, issuer, self.data_source, index_name_init=index_name_init)

    def main(self):
        page_info_list = self.parse_page_info()
        print(page_info_list)
        self.save_info_into_db(page_info_list)

if __name__ == '__main__':
    time_start = time.time()
    go = CollectAllIndexesFromRiceQuant()
    go.main()
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))