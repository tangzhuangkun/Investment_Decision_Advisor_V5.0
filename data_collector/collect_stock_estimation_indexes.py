#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import json
import sys
import time
import datetime
import requests
import akshare as ak

sys.path.append("..")
import parsers.disguise as disguise
import log.custom_logger as custom_logger
import db_mapper.financial_data.stocks_main_estimation_indexes_mapper as stocks_main_estimation_indexes_mapper


"""
从互联网爬取股票的估值信息
"""
class CollectStockEstimationIndexes:


    def __init__(self):
        pass

    """
    调用接口，从akshare获取单个股票估值信息（含 交易日， 市盈率TTM， 市净率， 市销率TTM， 股息率， 股息率TTM，总市值）
    : return,  将数据存入数据库
    """
    def call_api_to_get_single_stock_estimation_from_akshare(self, stock_code, stock_name, exchange_location, exchange_location_mic):

        # 获取该股票存储的最新日期
        stock_latest_date = stocks_main_estimation_indexes_mapper.StocksMainEstimationIndexesMapper().get_the_latest_date_of_stock(stock_code, exchange_location, exchange_location_mic)["p_day"]

        # 从akshare接口获取数据
        stock_a_indicator_lg_df = ak.stock_a_indicator_lg(symbol=stock_code)
        # 将行数据倒序，按时间由近及远
        df = stock_a_indicator_lg_df.iloc[::-1]

        # 如果日期为空
        if stock_latest_date == None:
            # 遍历数据，直接全部存储
            for index, row in df.iterrows():
                stocks_main_estimation_indexes_mapper.StocksMainEstimationIndexesMapper().save_akshare_stock_estimation(stock_code, stock_name, exchange_location, exchange_location_mic, row['trade_date'], row['pe_ttm'], row['pb'], row['ps_ttm'], row['dv_ratio'], row['dv_ttm'],row['total_mv'])
            # 日志记录
            msg = "从akshare的API-stock_a_indicator_lg 获取" + stock_name + " "+ stock_code + " 的全部历史交易日， 市盈率TTM， 市净率， 市销率TTM， 股息率， 股息率TTM，总市值 信息"
            custom_logger.CustomLogger().log_writter(msg, lev='info')
        else:
            # 遍历数据
            for index, row in df.iterrows():
                # 因为时间已经倒序，只收集更新日期的数据
                if row['trade_date']>stock_latest_date:
                    stocks_main_estimation_indexes_mapper.StocksMainEstimationIndexesMapper().save_akshare_stock_estimation(
                        stock_code, stock_name, exchange_location, exchange_location_mic, row['trade_date'], row['pe_ttm'],
                        row['pb'], row['ps_ttm'], row['dv_ratio'], row['dv_ttm'], row['total_mv'])
                # 如果时间与数据库中的日期一致或小于数据库中的日期，舍弃，跳出循环
                else:
                   break
            # 日志记录
            msg = "从akshare的API-stock_a_indicator_lg 获取" + stock_name + " " + stock_code + " 从 " + str(stock_latest_date + datetime.timedelta(days = 1)) + "至" + str(df.iloc[0]["trade_date"])+ "的全部历史交易日， 市盈率TTM， 市净率， 市销率TTM， 股息率， 股息率TTM，总市值 信息"
            custom_logger.CustomLogger().log_writter(msg, lev='info')

    def main(self):
        #self.call_api_to_get_single_stock_estimation_from_akshare("600519", "贵州茅台", "sh", "XSHG")
        #self.call_api_to_get_single_stock_estimation_from_akshare("000568", "泸州老窖", "sz", "XSHE")
        self.call_api_to_get_single_stock_estimation_from_akshare("000596", "古井贡酒", "sz", "XSHE")


if __name__ == '__main__':
    time_start = time.time()
    go = CollectStockEstimationIndexes()
    go.main()
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))
