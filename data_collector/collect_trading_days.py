#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import time
import akshare
import datetime

import sys
sys.path.append("..")
import log.custom_logger as custom_logger
import db_mapper.financial_data.trading_days_mapper as trading_days_mapper


class CollectTradingDays:
    # 从接口收集交易日

    def __init__(self):
        self.area = "中国大陆"
        self.source = "akshare"

    def collect_all_trading_days(self):
        # 从akshare获取所有的交易日期

        try:
            # 从从akshare接口获取所有的交易日期
            tool_trade_date_hist_sina_df = akshare.tool_trade_date_hist_sina()
            # 从panda dataframe格式转为list
            all_trade_dates_list = tool_trade_date_hist_sina_df["trade_date"].tolist()
            return all_trade_dates_list
        except Exception as e:
            # 日志记录
            msg = '无法从akshare获取所有的交易日期' + '  ' + str(e)
            custom_logger.CustomLogger().log_writter(msg, 'error')

    def save_a_trading_day_into_db(self, trading_day):
        '''
        将一个交易日期存入数据库
        :param trading_day: 交易日期，如 2021-06-09
        :return:
        '''

        # 是否存入成功的标志
        flag = True

        # 检查是否曾经存过该日期
        existOrNot = trading_days_mapper.TradingDaysMapper().is_saved_the_date(trading_day)
        # 如果没有该日期的记录，则存入
        if existOrNot is None:
            # 插入sql
            # inserting_sql = "INSERT IGNORE INTO trading_days (trading_date, area, source) VALUES ('%s', '%s', '%s')" \
            #                 % (trading_day, "中国大陆", "akshare")
            # # 将数据存入数据库
            # db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)
            trading_days_mapper.TradingDaysMapper().save_trading_date(trading_day, self.area, self.source)
            return flag
        # 如果该日期存在记录，则返回
        else:
            flag = False
            return flag

    def save_all_trading_days_into_db(self):
        # 将获取到的，截止至今日的交易日期存入数据可

        # 获取所有的交易日期
        all_trading_days_list = self.collect_all_trading_days()

        # 获取当前日期
        today = datetime.date.today()
        # 倒序遍历，从近的日期向远的日期遍历
        for i in range(len(all_trading_days_list)-1,-1,-1):
            # 如果日期大于今天日期，则略过
            if(all_trading_days_list[i]>today):
                continue
            # 否则存入数据库
            else:
                isSavedSuccessfully = self.save_a_trading_day_into_db(all_trading_days_list[i])
                # 如果存入失败，说明该交易日期已存在数据库中，往前的日期也在存在，无需再重复插入
                if not isSavedSuccessfully:
                    break

    def main(self):
        self.save_all_trading_days_into_db()

if __name__ == '__main__':
    time_start = time.time()
    go = CollectTradingDays()
    #result = go.collect_all_trading_days()
    #print(result)
    #go.save_all_trading_days_into_db()
    #existOrNot = go.is_saved_or_not("2021-06-04")
    #print(existOrNot)
    go.main()
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))