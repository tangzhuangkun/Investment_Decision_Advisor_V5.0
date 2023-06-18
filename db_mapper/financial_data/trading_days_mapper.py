#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun



import time
import sys

sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger


"""
数据表，trading_days 的映射
以该表为主的数据操作，均在此完成
"""

class TradingDaysMapper:

    def __init__(self):
        pass

    """
    获取传入日期参数最近的交易日期, 即上一个交易日;
    如果今天有交易，是收盘后调取，则获取的是，今天的交易日期
    仅限于A股
    :param, p_day: 交易日期，如 2021-06-09
    :return, 如果存在最近的交易日期，则返回日期
            如果不存在，则返回 0000-00-00
    """
    def get_the_lastest_trading_date(self,p_day):
        # 查询SQL
        selecting_sql = """SELECT trading_date as p_day FROM trading_days WHERE trading_date <= '%s' 
        ORDER BY ABS(DATEDIFF(trading_date, '%s')) LIMIT 1 """ % (p_day,p_day)
        # 查询
        selecting_result = db_operator.DBOperator().select_one("financial_data", selecting_sql)

        # 如果查询得到结果
        if selecting_result is not None:
            return str(selecting_result["p_day"])
        # 如果查询不到结果
        else:
            # 日志记录
            log_msg = "无法获取 "+p_day+" 最近的交易日期"
            custom_logger.CustomLogger().log_writter(log_msg, 'error')
            return "0000-00-00"

    '''
    检查一个交易日期是否已经存在
    :param trading_day: 交易日期，如 2021-06-09
    :return:
    '''
    def is_saved_the_date(self,p_day):
        try:
            # 查询sql
            selecting_sql = "SELECT * FROM trading_days WHERE trading_date = '%s'" % (p_day)
            # 查询
            selecting_result = db_operator.DBOperator().select_one("financial_data", selecting_sql)
            return selecting_result

        except Exception as e:
            # 日志记录
            msg = "无法查询交易日期 " + p_day + " 是否存在数据库" + '  ' + str(e)
            custom_logger.CustomLogger().log_writter(msg, 'error')
            return None

    def save_trading_date(self,p_day, area, source):

        try:
            # 插入sql
            inserting_sql = "INSERT IGNORE INTO trading_days (trading_date, area, source) VALUES ('%s', '%s', '%s')" \
                            % (p_day, area, source)
            # 将数据存入数据库
            db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)
        except Exception as e:
            # 日志记录
            msg = "无法存入 " + source + " " + area+ " "+p_day+ " 的交易日信息进数据库 "+ str(e)
            custom_logger.CustomLogger().log_writter(msg, 'error')

if __name__ == '__main__':
    time_start = time.time()
    go = TradingDaysMapper()
    #result = go.get_the_lastest_trading_date("2023-05-12")
    result = go.is_saved_the_date("2023-05-12")
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)