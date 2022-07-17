#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import sys
import time

sys.path.append("..")
import log.custom_logger as custom_logger
import database.db_operator as db_operator
import data_collector.get_target_real_time_indicator_from_interfaces as get_target_real_time_indicator_from_interfaces
import data_miner.data_miner_common_db_operator as data_miner_common_db_operator

class TimeStrategyRealtimeEquityBondYield:
    # 择时策略，估算实时股债收益率
    # 沪深300指数市值加权估值PE/十年国债收益率
    # 用于判断股市收益率与无风险收益之间的比值
    # 频率：每个交易日，盘中

    def __init__(self):
        # 当天的日期
        self.today = time.strftime("%Y-%m-%d", time.localtime())

    def get_realtime_CSI_300_change(self):
        # 获取沪深300指数的实时涨跌幅
        change = get_target_real_time_indicator_from_interfaces.GetTargetRealTimeIndicatorFromInterfaces().get_single_target_real_time_indicator("sh000300","change")
        return change

    def get_last_trading_day_CSI_300_yield_rate(self):
        # 获取上个交易日的沪深300收益率
        last_trading_date = data_miner_common_db_operator.DataMinerCommonDBOperator().get_the_last_trading_date(self.today)

        selecting_sql = "select stock_yield_rate from stock_bond_ratio_di " \
                        "where index_code = '000300' and trading_date = '%s' " % (last_trading_date)
        # 查询
        csi_300_last_trading_date_yield_rate = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        return csi_300_last_trading_date_yield_rate["stock_yield_rate"]

    """
    # TODO
    实时10年期国债，应该从
    get方式
    http://stock.finance.sina.com.cn/forex/globalbd/gcny10.html，相关的 
    http://hq.sinajs.cn/?rn=1658068973800&list=globalbd_gcny10
    
    
    post方式
    https://www.chinamoney.com.cn/chinese/bkcurvrty/，相关的
    https://www.chinamoney.com.cn/ags/ms/cm-u-bk-currency/RtimeYldCurv?lang=CN
    
    """




if __name__ == '__main__':
    time_start = time.time()
    go = TimeStrategyRealtimeEquityBondYield()
    result = go.get_realtime_CSI_300_ETF_change()
    #result = go.get_last_trading_day_CSI_300_yield_rate()
    print(result)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))