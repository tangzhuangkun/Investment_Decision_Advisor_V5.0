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
        # return : 1.04
        change = get_target_real_time_indicator_from_interfaces.GetTargetRealTimeIndicatorFromInterfaces().get_single_target_real_time_indicator("sh000300","change")
        return change

    def get_last_trading_day_CSI_300_yield_rate(self):
        # 获取上个交易日的沪深300收益率
        # return: 0.081500
        last_trading_date = data_miner_common_db_operator.DataMinerCommonDBOperator().get_the_last_trading_date(self.today)

        selecting_sql = "select stock_yield_rate from stock_bond_ratio_di " \
                        "where index_code = '000300' and trading_date = '%s' " % (last_trading_date)
        # 查询
        csi_300_last_trading_date_yield_rate = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        return csi_300_last_trading_date_yield_rate["stock_yield_rate"]

    def calculate_realtime_equity_bond_yield(self):
        # 基于沪深300指数，计算实时的股债收益比
        # return : 0.0806524

        # 获取上个交易日的沪深300收益率
        last_trading_day_CSI_300_yield_rate = float(self.get_last_trading_day_CSI_300_yield_rate())
        # 获取沪深300指数的实时涨跌幅
        realtime_CSI_300_change = float(self.get_realtime_CSI_300_change())
        # 获取实时十年期国债收益率
        realtime_ten_year_treasury_yield = float(get_target_real_time_indicator_from_interfaces.GetTargetRealTimeIndicatorFromInterfaces().get_real_time_treasury_yield("globalbd_gcny10"))
        # 计算实时股债收益比
        realtime_equity_bond_yield = last_trading_day_CSI_300_yield_rate*(100-realtime_CSI_300_change)/100/realtime_ten_year_treasury_yield
        return realtime_equity_bond_yield

if __name__ == '__main__':
    time_start = time.time()
    go = TimeStrategyRealtimeEquityBondYield()
    #result = go.get_realtime_CSI_300_change()
    #result = go.get_last_trading_day_CSI_300_yield_rate()
    result = go.calculate_realtime_equity_bond_yield()
    print(result)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))