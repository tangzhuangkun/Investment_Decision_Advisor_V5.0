#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import time
import sys

sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger

class DataMinerCommonDBOperator:
    # 通用，常用的（非基金或股票信息）数据库操作

    def __init__(self):
        pass

    def get_the_lastest_trading_date(self,day):
        # 获取传入日期参数最近的交易日期, 即上一个交易日;
        # 如果今天有交易，是收盘后调取，则获取的是，今天的交易日期
        # 仅限于A股
        # day: 交易日期，如 2021-06-09
        # return: 如果存在最近的交易日期，则返回日期
        #         如果不存在，则返回 0000-00-00

        # 查询SQL
        selecting_sql = "SELECT trading_date FROM trading_days WHERE trading_date <= '%s' ORDER BY " \
                        "ABS(DATEDIFF(trading_date, '%s')) ASC LIMIT 1" % (day,day)

        # 查询
        selecting_result = db_operator.DBOperator().select_one("financial_data", selecting_sql)

        if selecting_result is not None:
            return str(selecting_result["trading_date"])
        else:
            # 日志记录
            log_msg = "无法获取 "+day+" 最近的交易日期"
            custom_logger.CustomLogger().log_writter(log_msg, 'error')
            return "0000-00-00"


    def get_all_tokens(self,platform_code):
        # 获取某个平台的全部令牌
        # platform_code: 平台代码，如 理杏仁的代码为 lxr
        # return: 如果存在令牌，则返回全部令牌,为有一个list
        #         如果不存在，则返回空列表， []

        # 返回的令牌列表
        token_list = list()

        # 查询SQL
        selecting_sql = "SELECT token FROM token_record WHERE platform_code = '%s' " % (platform_code)
        # 查询
        selecting_result = db_operator.DBOperator().select_all("parser_component", selecting_sql)

        # 将令牌从dict转为list
        for token_unit in selecting_result:
            token_list.append(token_unit['token'])
        return token_list


    def get_one_token(self,platform_code):
        # 随机获取某个平台的一个令牌
        # platform_code: 平台代码，如 理杏仁的代码为 lxr

        # 查询SQL
        selecting_sql = "SELECT token FROM token_record WHERE platform_code = '%s' ORDER BY RAND() LIMIT 1" % (platform_code)
        # 查询
        selecting_result = db_operator.DBOperator().select_one("parser_component", selecting_sql)

        return selecting_result["token"]

if __name__ == '__main__':
    time_start = time.time()
    go = DataMinerCommonDBOperator()
    #last_trade_day = go.get_the_lastest_trading_date("2022-03-20")
    #print(last_trade_day)
    #token_list = go.get_all_tokens("lxr")
    #print(token_list)
    token = go.get_one_token("lxr")
    print(token)

    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))