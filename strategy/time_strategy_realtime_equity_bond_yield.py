#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import sys
import time
import decimal

sys.path.append("..")
import database.db_operator as db_operator
import data_collector.get_target_real_time_indicator_from_interfaces as get_target_real_time_indicator_from_interfaces
import data_miner.data_miner_common_db_operator as data_miner_common_db_operator
import data_miner.data_miner_common_target_index_operator as data_miner_common_target_index_operator

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

    def estimate_realtime_equity_bond_yield(self):
        # 基于沪深300指数，估算实时的股债收益比
        # return : 0.0806524

        # 获取上个交易日的沪深300收益率
        last_trading_day_CSI_300_yield_rate = float(self.get_last_trading_day_CSI_300_yield_rate())
        # 获取沪深300指数的实时涨跌幅
        realtime_CSI_300_change = float(self.get_realtime_CSI_300_change())/100
        # 获取实时十年期国债收益率
        realtime_ten_year_treasury_yield = float(get_target_real_time_indicator_from_interfaces.GetTargetRealTimeIndicatorFromInterfaces().get_real_time_treasury_yield("globalbd_gcny10"))/100
        # 预估当前沪深300收益率
        estimate_realtime_CSI_300_yield_rate = last_trading_day_CSI_300_yield_rate / (1 + realtime_CSI_300_change)
        # 计算实时股债收益比
        realtime_equity_bond_yield = estimate_realtime_CSI_300_yield_rate / realtime_ten_year_treasury_yield
        return realtime_equity_bond_yield


    def estimate_current_realtime_equity_bond_yield_rank(self, yearCount, realtime_CSI_300_yield_rate):
        # 估算当前实时股债收益比的与历史数据相比的排位
        # yearCount： 年数，时间跨度，必须为整数，如 2
        # realtime_CSI_300_yield_rate: 实时股债收益比, 如 0.0806524
        # return：年数, 当前实时股债收益比, 历史排位百分比, 如   (2, 2.8855, 87.81)
        selecting_sql = "select ratio from stock_bond_ratio_di " \
                        "where index_code = '000300' and trading_date > subdate(now(), interval '%s' year) " \
                        "order by ratio desc" % (yearCount)

        # 查询历史上所有股债比
        all_historical_ratio_list = db_operator.DBOperator().select_all("aggregated_data", selecting_sql)
        # 历史上总共有多少数据量
        all_historical_ratio_count = len(all_historical_ratio_list)
        # 如果是极端值，大于历史上的最大值
        if(realtime_CSI_300_yield_rate>=all_historical_ratio_list[0]["ratio"]):
            return (yearCount, round(float(realtime_CSI_300_yield_rate),4),100)
        # 如果是极端值，小于历史上的最小值
        elif(realtime_CSI_300_yield_rate<all_historical_ratio_list[all_historical_ratio_count-1]["ratio"]):
            return (yearCount, round(float(realtime_CSI_300_yield_rate),4), 0)
        # 处于0%-100%之间
        else:
            for ratio_index in range(all_historical_ratio_count):
                if(realtime_CSI_300_yield_rate>all_historical_ratio_list[ratio_index]["ratio"]):
                    return (yearCount, round(float(realtime_CSI_300_yield_rate),4), round(((1-ratio_index/all_historical_ratio_count)*100),2))


    def generate_pure_notification_msg(self):
        # 单纯生成统计数据的通知信息

        # 当前时间
        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        # 实时股债收益比
        realtime_CSI_300_yield_rate = decimal.Decimal(self.estimate_realtime_equity_bond_yield())

        # 当前实时预估的股债收益比在近3，5，8，10年的排位信息
        # 年数, 当前实时股债收益比, 历史排位百分比, 如   (2, 2.8855, 87.81)
        three_year_equity_bond_yield_info = self.estimate_current_realtime_equity_bond_yield_rank(3, realtime_CSI_300_yield_rate)
        five_year_equity_bond_yield_info = self.estimate_current_realtime_equity_bond_yield_rank(5, realtime_CSI_300_yield_rate)
        eight_year_equity_bond_yield_info = self.estimate_current_realtime_equity_bond_yield_rank(8,realtime_CSI_300_yield_rate)
        ten_year_equity_bond_yield_info = self.estimate_current_realtime_equity_bond_yield_rank(10,realtime_CSI_300_yield_rate)
        # 当前预估的实时股债收益比
        estimate_realtime_equity_bond_yield = three_year_equity_bond_yield_info[1]

        # 生成的消息
        msg = ''
        msg += current_time + ' \n'
        msg += '基于沪深300指数' + ' \n'
        msg += '预估实时股债收益比： ' + str(estimate_realtime_equity_bond_yield) + ' \n'
        msg += '近3年历史排位： ' + str(three_year_equity_bond_yield_info[2]) + ' %' + ' \n'
        msg += '近5年历史排位： ' + str(five_year_equity_bond_yield_info[2]) + ' %' + ' \n'
        msg += '近8年历史排位： ' + str(eight_year_equity_bond_yield_info[2]) + ' %' + ' \n'
        msg += '近10年历史排位： ' + str(ten_year_equity_bond_yield_info[2]) + ' %' + ' \n'

        return msg

    def generate_investment_notification_msg(self):
        # 触发阈值时，生成基于统计数据的投资决策通知信息

        # 当前时间
        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        # 获取触发值信息
        # 如 {'trigger_value': Decimal('3.00'), 'trigger_percent': Decimal('95.00')}
        trigger_info = data_miner_common_target_index_operator.DataMinerCommonTargetIndexOperator().get_given_index_trigger_info("diy_000300_cn10yr","equity_bond_yield")
        # 如果无任何触发值信息
        if trigger_info==None:
            return None

        # 触发值
        trigger_value = trigger_info["trigger_value"]
        # 触发百分比
        trigger_percent = trigger_info["trigger_percent"]

        # 实时股债收益比
        realtime_CSI_300_yield_rate = decimal.Decimal(self.estimate_realtime_equity_bond_yield())

        # 当前实时预估的股债收益比在近3，5，8，10年的排位信息
        # 年数, 当前实时股债收益比, 历史排位百分比, 如   (3, 2.8855, 87.81)
        three_year_equity_bond_yield_info = self.estimate_current_realtime_equity_bond_yield_rank(3, realtime_CSI_300_yield_rate)
        five_year_equity_bond_yield_info = self.estimate_current_realtime_equity_bond_yield_rank(5, realtime_CSI_300_yield_rate)
        eight_year_equity_bond_yield_info = self.estimate_current_realtime_equity_bond_yield_rank(8, realtime_CSI_300_yield_rate)
        ten_year_equity_bond_yield_info = self.estimate_current_realtime_equity_bond_yield_rank(10, realtime_CSI_300_yield_rate)
        # three_year_equity_bond_yield_info = (3, 3.8855, 95.33)
        # five_year_equity_bond_yield_info = (5, 3.8855, 95.55)
        # eight_year_equity_bond_yield_info = (8, 3.8855, 95.88)
        # ten_year_equity_bond_yield_info = (10, 3.8855, 95.00)

        # 当前预估的实时股债收益比
        realtime_CSI_300_yield_rate = three_year_equity_bond_yield_info[1]
        # 生成的消息
        msg = ''
        if(realtime_CSI_300_yield_rate>=trigger_value or three_year_equity_bond_yield_info[2]>=trigger_percent or five_year_equity_bond_yield_info[2]>=trigger_percent or eight_year_equity_bond_yield_info[2]>=trigger_percent or ten_year_equity_bond_yield_info[2]>=trigger_percent):
            msg += current_time + ' \n'
            msg += '基于沪深300指数' + ' \n'
            if(realtime_CSI_300_yield_rate>=trigger_value):
                msg += '预估实时股债收益比大于阈值3： '  + str(realtime_CSI_300_yield_rate) + ' \n'
            else:
                msg += '预估实时股债收益比： ' + str(realtime_CSI_300_yield_rate) + ' \n'
            if(three_year_equity_bond_yield_info[2]>=trigger_percent):
                msg += '近3年历史排位大于阈值' + str(trigger_percent) +  ":  " + str(three_year_equity_bond_yield_info[2]) + ' %' + ' \n'
            else:
                msg += '近3年历史排位： ' + str(three_year_equity_bond_yield_info[2]) + ' %' + ' \n'
            if (five_year_equity_bond_yield_info[2] >= trigger_percent):
                msg += '近5年历史排位大于阈值' + str(trigger_percent) +  ":  " + str(five_year_equity_bond_yield_info[2]) + ' %' + ' \n'
            else:
                msg += '近5年历史排位： ' + str(five_year_equity_bond_yield_info[2]) + ' %' + ' \n'
            if (eight_year_equity_bond_yield_info[2] >= trigger_percent):
                msg += '近8年历史排位大于阈值' + str(trigger_percent) +  ":  " + str(eight_year_equity_bond_yield_info[2]) + ' %' + ' \n'
            else:
                msg += '近8年历史排位： ' + str(eight_year_equity_bond_yield_info[2]) + ' %' + ' \n'
            if (ten_year_equity_bond_yield_info[2] >= trigger_percent):
                msg += '近10年历史排位大于阈值' + str(trigger_percent) +  ":  " + str(ten_year_equity_bond_yield_info[2]) + ' %' + ' \n'
            else:
                msg += '近10年历史排位： ' + str(ten_year_equity_bond_yield_info[2]) + ' %' + ' \n'
        else:
            return None

        return msg

    def main(self):
        # 主入口

        # 是否存在需要执行的股债收益比策略
        is_exist_equity_bond_yield_strategy = data_miner_common_target_index_operator.DataMinerCommonTargetIndexOperator().index_valuated_by_method("equity_bond_yield")
        # 如果存在需要执行的策略
        if(is_exist_equity_bond_yield_strategy):
            return self.generate_investment_notification_msg()
        # 如果不存在需要执行的策略
        else:
            return None


if __name__ == '__main__':
    time_start = time.time()
    go = TimeStrategyRealtimeEquityBondYield()
    #result = go.get_realtime_CSI_300_change()
    #result = go.get_last_trading_day_CSI_300_yield_rate()
    #result = go.calculate_realtime_equity_bond_yield()
    #result =go.estimate_current_realtime_equity_bond_yield_rank(3)
    #result = go.generate_pure_notification_msg()
    #result = go.generate_investment_notification_msg()
    result = go.main()
    print(result)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))