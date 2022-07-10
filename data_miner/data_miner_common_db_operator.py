#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import time
import sys

sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger

class DataMinerCommonDBOperation:
    # 通用，常用的（非基金或股票信息）数据库操作

    def __init__(self):
        pass

    def get_the_last_trading_date(self,day):
        # 获取传入日期参数最近的交易日期, 即上一个交易日
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


if __name__ == '__main__':
    time_start = time.time()
    go = DataMinerCommonDBOperation()
    last_trade_day = go.get_the_last_trading_date("2022-03-20")
    print(last_trade_day)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))