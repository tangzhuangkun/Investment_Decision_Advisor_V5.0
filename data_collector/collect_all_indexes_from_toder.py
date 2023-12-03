#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun
import time
import urllib.request
import json
import log.custom_logger as custom_logger

import sys
sys.path.append("..")
import db_mapper.financial_data.fin_data_indexes_list_mapper as fin_data_indexes_list_mapper


class CollectAllIndexesFromToder:
    # 从拓观网收集 中国所有的指数列表
    # 运行频率：指定运行

    def __init__(self):
        self.toder_url = "https://www.todergroup.com/marketdata/api/indices/?indices=&sort=ticker_asc&page="
        self.data_source = "拓观"


    """
    爬取并解析拓观网的指数列表某一页面
    :param, page_num: 页码, 默认为1
    """
    def parse_page_info(self, page_num=1):
        # 页面信息指数列表
        page_info_list = list()
        response = urllib.request.urlopen(self.toder_url+str(page_num))
        json_data = json.loads(response.read().decode('utf-8'))
        # 如果返回的内容不为空，则开始解析
        if(len(json_data.get('items'))>0):
            for item in json_data.get('items'):
                item_dict = dict()
                for index_info in item.get('data'):
                    # 获取指数代码
                    if(index_info.get('name')=='ticker'):
                        item_dict["index_code"] = index_info.get('value')
                    # 获取指数名称
                    if (index_info.get('name') == 'indexName'):
                        item_dict["index_name"] = index_info.get('value')
                    # 获取指数包含成分股个数
                    if (index_info.get('name') == 'numberOfSecurities'):
                        item_dict["securities_num"] = index_info.get('value')
                    # 获取指数发行人
                    if (index_info.get('name') == 'issuer'):
                        item_dict["issuer"] = index_info.get('value')
                page_info_list.append(item_dict)
        return page_info_list

    """
    将页面中的指数信息存入数据库
    :param, page_info_list，页面中的指数列表信息， 如 [{'index_code': '000001', 'index_name': '上证指数', 'securities_num': 2137, 'issuer': '上海证券交易所'}, {'index_code': '000001CNY01', 'index_name': '上证指数全收益', 'securities_num': 2137, 'issuer': '中证指数有限公司'},,,, ]
    """
    def save_info_into_db(self, page_info_list):
        for item in page_info_list:
            index_code = item.get('index_code')
            index_name = item.get('index_name')
            securities_num = item.get('securities_num')
            issuer = item.get('issuer')
            fin_data_indexes_list_mapper.FinDataIndexesListMapper().insert_all_indexes(index_code, index_name, issuer, self.data_source, securities_num)

    """
    收集所有页面的指数信息
    """
    def main(self):
        # 收集数据前，先清除数据库中同一源的数据记录，避免重复
        fin_data_indexes_list_mapper.FinDataIndexesListMapper().delete_specific_source_record(self.data_source)
        # 从第一页开始
        page_num = 1
        # 标志位，该页是否有内容
        hasPageContentFlag = True
        # 如果该页有内容，则继续收集下一页
        while (hasPageContentFlag):
            # 获取页面信息
            page_info_list = self.parse_page_info(page_num)
            # 储存该页信息
            self.save_info_into_db(page_info_list)
            # 如果该页存在内容
            if(len(page_info_list)>0):
                page_num += 1
            # 如果该页无内容
            else:
                hasPageContentFlag = False
        # 日志记录
        msg = " 从拓观网" + self.toder_url + '  ' + "获取指数列表"
        custom_logger.CustomLogger().log_writter(msg, lev='info')




if __name__ == '__main__':
    time_start = time.time()
    go = CollectAllIndexesFromToder()
    go.main()
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))
