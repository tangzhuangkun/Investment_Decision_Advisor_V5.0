#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import sys
import time
import decimal

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
        realtime_equity_bond_yield = last_trading_day_CSI_300_yield_rate*(100-realtime_CSI_300_change)/realtime_ten_year_treasury_yield
        return realtime_equity_bond_yield


    def calculate_current_realtime_equity_bond_yield_rank(self):
        # 计算当前实时股债收益比的与历史数据相比的排位
        # return：当前实时股债收益比和历史排位百分比  (2.8771481412772033, 83.88)
        selecting_sql = "select ratio from stock_bond_ratio_di " \
                        "where index_code = '000300' order by ratio desc"
        # 查询
        all_historical_ratio_list = db_operator.DBOperator().select_all("aggregated_data", selecting_sql)
        # 实时股债收益比
        realtime_equity_bond_yield_decimal = decimal.Decimal(self.calculate_realtime_equity_bond_yield())
        # 历史上总共有多少数据量
        all_historical_ratio_count = len(all_historical_ratio_list)

        # 如果是极端值，大于历史上的最大值
        if(realtime_equity_bond_yield_decimal>=all_historical_ratio_list[0]["ratio"]):
            return (float(realtime_equity_bond_yield_decimal),100)
        # 如果是极端值，小于历史上的最小值
        elif(realtime_equity_bond_yield_decimal<all_historical_ratio_list[all_historical_ratio_count-1]["ratio"]):
            return (float(realtime_equity_bond_yield_decimal), 0)
        # 处于0%-100%之间
        else:
            for ratio_index in range(all_historical_ratio_count):
                if(realtime_equity_bond_yield_decimal>all_historical_ratio_list[ratio_index]["ratio"]):
                    return (float(realtime_equity_bond_yield_decimal), round(((1-ratio_index/all_historical_ratio_count)*100),2))

if __name__ == '__main__':
    time_start = time.time()
    go = TimeStrategyRealtimeEquityBondYield()
    #result = go.get_realtime_CSI_300_change()
    #result = go.get_last_trading_day_CSI_300_yield_rate()
    #result = go.calculate_realtime_equity_bond_yield()
    result =go.calculate_current_realtime_equity_bond_yield_rank()
    print(result)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))