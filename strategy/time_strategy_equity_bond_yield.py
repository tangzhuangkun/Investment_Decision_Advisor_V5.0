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
import data_collector.collect_chn_gov_bonds_rates as collect_chn_gov_bonds_rates
import data_collector.collect_index_estimation_from_lxr as collect_index_estimation_from_lxr
import data_miner.calculate_stock_bond_ratio as calculate_stock_bond_ratio
import db_mapper.target_pool.investment_target_mapper as investment_target_mapper

class TimeStrategyEquityBondYield:
    # 择时策略，估算实时股债收益率
    # 沪深300指数市值加权估值PE/十年国债收益率
    # 用于判断股市收益率与无风险收益之间的比值
    # 频率：每个交易日，盘中

    def __init__(self):
        pass

    def prepare_index_estimation_bond_rate_and_cal_yield(self):
        # 准备数据，收集最新沪深300指数市值加权估值和国债利率,
        # 并计算当日收盘后的真实股债收益率
        # 盘后执行

        # 收集最新国债收益率
        collect_chn_gov_bonds_rates.CollectCHNGovBondsRates().main()
        # 收集最新沪深300指数市值加权估值
        collect_index_estimation_from_lxr.CollectIndexEstimationFromLXR().main()
        # 运行mysql脚本，计算股债收益率
        calculate_stock_bond_ratio.CalculateStockBondRatio().main()


    def get_realtime_CSI_300_change(self):
        # 获取沪深300指数的实时涨跌幅
        # return : 1.04
        change = get_target_real_time_indicator_from_interfaces.GetTargetRealTimeIndicatorFromInterfaces().get_single_target_real_time_indicator("sh000300","change")
        return change

    def get_the_lastest_trading_day_CSI_300_yield_rate(self):
        # 获取最近交易日的沪深300收益率，
        # 如果今天有开市，收盘之后调用，则是今天的收益率
        # return: 0.081500

        # 当天的日期
        today = time.strftime("%Y-%m-%d", time.localtime())
        last_trading_date = data_miner_common_db_operator.DataMinerCommonDBOperator().get_the_lastest_trading_date(today)

        selecting_sql = "select stock_yield_rate from stock_bond_ratio_di " \
                        "where index_code = '000300' and trading_date = '%s' " % (last_trading_date)
        # 查询
        csi_300_last_trading_date_yield_rate = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        return csi_300_last_trading_date_yield_rate["stock_yield_rate"]

    def get_the_lastest_trading_day_CSI_300_yield_ratio(self):
        # 获取最近交易日的股债收益率，
        # 如果今天有开市，收盘之后调用，则是今天的股债收益率
        # return: 2.91811984212414770

        # 当天的日期
        today = time.strftime("%Y-%m-%d", time.localtime())
        last_trading_date = data_miner_common_db_operator.DataMinerCommonDBOperator().get_the_lastest_trading_date(
            today)

        selecting_sql = "select ratio from stock_bond_ratio_di " \
                        "where index_code = '000300' and trading_date = '%s' " % (last_trading_date)
        # 查询
        the_lastest_trading_date_ratio = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        return the_lastest_trading_date_ratio["ratio"]

    def estimate_realtime_equity_bond_yield(self):
        # 基于沪深300指数，估算实时的股债收益比
        # return : 2.9031198686371105

        # 获取上个交易日的沪深300收益率
        last_trading_day_CSI_300_yield_rate = float(self.get_the_lastest_trading_day_CSI_300_yield_rate())
        # 获取沪深300指数的实时涨跌幅
        realtime_CSI_300_change = float(self.get_realtime_CSI_300_change())/100
        # 获取实时十年期国债收益率
        realtime_ten_year_treasury_yield = float(get_target_real_time_indicator_from_interfaces.GetTargetRealTimeIndicatorFromInterfaces().get_real_time_treasury_yield("globalbd_gcny10"))/100
        # 预估当前沪深300收益率
        estimate_realtime_CSI_300_yield_rate = last_trading_day_CSI_300_yield_rate / (1 + realtime_CSI_300_change)
        # 计算实时股债收益比
        realtime_equity_bond_yield = estimate_realtime_CSI_300_yield_rate / realtime_ten_year_treasury_yield
        return realtime_equity_bond_yield


    def cal_equity_bond_yield_historical_rank(self, year_count, yield_ratio):
        # 估算当前实时股债收益比的与历史数据相比的排位
        # year_count： 年数，时间跨度，必须为整数，如 2
        # yield_ratio: 股债收益比, 如 0.0806524
        # return：年数, 股债收益比, 历史排位百分比, 如   (2, 2.8855, 87.81)
        selecting_sql = "select ratio from stock_bond_ratio_di " \
                        "where index_code = '000300' and trading_date > subdate(now(), interval '%s' year) " \
                        "order by ratio desc" % (year_count)

        # 查询历史上所有股债比
        all_historical_ratio_list = db_operator.DBOperator().select_all("aggregated_data", selecting_sql)
        # 历史上总共有多少数据量
        all_historical_ratio_count = len(all_historical_ratio_list)
        # 如果是极端值，大于历史上的最大值,100%情况
        if(yield_ratio>=all_historical_ratio_list[0]["ratio"]):
            return (year_count, round(float(yield_ratio),8),100)
        # 如果是极端值，小于历史上的最小值,0%情况
        elif(yield_ratio<all_historical_ratio_list[all_historical_ratio_count-1]["ratio"]):
            return (year_count, round(float(yield_ratio),8), 0)
        # 处于0%-100%之间
        else:
            for ratio_index in range(all_historical_ratio_count):
                if(yield_ratio>all_historical_ratio_list[ratio_index]["ratio"]):
                    return (year_count, round(float(yield_ratio),8), round(((1-ratio_index/all_historical_ratio_count)*100),2))


    def generate_today_notification_msg(self):
        # 每日收盘后，单纯生成统计数据的通知信息

        # 准备数据，收集最新沪深300指数市值加权估值和国债利率,
        # 并计算当日收盘后的真实股债收益率
        self.prepare_index_estimation_bond_rate_and_cal_yield()

        # 当天的日期
        today = time.strftime("%Y-%m-%d", time.localtime())
        # 今天收盘的股债收益比
        today_ratio = self.get_the_lastest_trading_day_CSI_300_yield_ratio()

        # 今天股债收益比在近3，5，8，10年的排位信息
        # 年数, 今天股债收益比, 历史排位百分比, 如   (2, 2.8855, 87.81)
        three_year_equity_bond_yield_info = self.cal_equity_bond_yield_historical_rank(3, today_ratio)
        five_year_equity_bond_yield_info = self.cal_equity_bond_yield_historical_rank(5, today_ratio)
        eight_year_equity_bond_yield_info = self.cal_equity_bond_yield_historical_rank(8,today_ratio)
        ten_year_equity_bond_yield_info = self.cal_equity_bond_yield_historical_rank(10,today_ratio)

        # 生成的消息
        msg = ''
        msg += today + ' \n'
        msg += '基于沪深300指数' + ' \n'
        msg += '股债收益比： ' + str(round(today_ratio,8)) + ' \n'
        msg += '近3年历史排位： ' + str(three_year_equity_bond_yield_info[2]) + ' %' + ' \n'
        msg += '近5年历史排位： ' + str(five_year_equity_bond_yield_info[2]) + ' %' + ' \n'
        msg += '近8年历史排位： ' + str(eight_year_equity_bond_yield_info[2]) + ' %' + ' \n'
        msg += '近10年历史排位： ' + str(ten_year_equity_bond_yield_info[2]) + ' %' + ' \n'

        return msg


    """
    # 可废弃，2023-06-23， 无需在盘中高频监控
    def generate_realtime_investment_notification_msg(self):
        # 触发阈值时，实时生成基于统计数据的投资决策通知信息

        # 当前时间
        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        # 获取触发值信息
        # 如 {'trigger_value': Decimal('3.00'), 'trigger_percent': Decimal('95.00')}
        #trigger_info = data_miner_common_target_index_operator.DataMinerCommonTargetIndexOperator().get_given_index_trigger_info("diy_000300_cn10yr","equity_bond_yield")
        trigger_info = investment_target_mapper.InvestmentTargetMapper().get_given_index_trigger_info("stock_bond", "diy_000300_cn10yr", "active", "equity_bond_yield", "buy")
        # 如果无任何触发值信息
        if trigger_info==None:
            return None

        # 触发值
        trigger_value = trigger_info["trigger_value"]
        # 触发百分比
        trigger_percent = trigger_info["trigger_percent"]

        # 当前预估的实时股债收益比
        realtime_CSI_300_yield_ratio = decimal.Decimal(self.estimate_realtime_equity_bond_yield())

        # 当前实时预估的股债收益比在近3，5，8，10年的排位信息
        # 年数, 当前实时股债收益比, 历史排位百分比, 如   (3, 2.8855, 87.81)
        three_year_equity_bond_yield_info = self.cal_equity_bond_yield_historical_rank(3, realtime_CSI_300_yield_ratio)
        five_year_equity_bond_yield_info = self.cal_equity_bond_yield_historical_rank(5, realtime_CSI_300_yield_ratio)
        eight_year_equity_bond_yield_info = self.cal_equity_bond_yield_historical_rank(8, realtime_CSI_300_yield_ratio)
        ten_year_equity_bond_yield_info = self.cal_equity_bond_yield_historical_rank(10, realtime_CSI_300_yield_ratio)
        # three_year_equity_bond_yield_info = (3, 3.8855, 95.33)
        # five_year_equity_bond_yield_info = (5, 3.8855, 95.55)
        # eight_year_equity_bond_yield_info = (8, 3.8855, 95.88)
        # ten_year_equity_bond_yield_info = (10, 3.8855, 95.00)

        # 生成的消息
        msg = ''
        if(realtime_CSI_300_yield_ratio>=trigger_value or three_year_equity_bond_yield_info[2]>=trigger_percent or five_year_equity_bond_yield_info[2]>=trigger_percent or eight_year_equity_bond_yield_info[2]>=trigger_percent or ten_year_equity_bond_yield_info[2]>=trigger_percent):
            msg += current_time + ' \n'
            msg += '基于沪深300指数' + ' \n'
            if(realtime_CSI_300_yield_ratio>=trigger_value):
                msg += '预估实时股债收益比大于阈值3： '  + str(round(realtime_CSI_300_yield_ratio,8)) + ' \n'
            else:
                msg += '预估实时股债收益比： ' + str(round(realtime_CSI_300_yield_ratio,8)) + ' \n'
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

    def if_generate_realtime_investment_notification_msg(self):
        # 是否 实时生成基于统计数据的投资决策通知信息

        # 是否存在需要执行的股债收益比策略
        #is_exist_equity_bond_yield_strategy = data_miner_common_target_index_operator.DataMinerCommonTargetIndexOperator().index_valuated_by_method("equity_bond_yield")
        is_exist_equity_bond_yield_strategy = investment_target_mapper.InvestmentTargetMapper().get_target_valuated_by_method("stock_bond", "equity_bond_yield", "active", "buy")
        # 如果存在需要执行的策略
        if(is_exist_equity_bond_yield_strategy):
            return self.generate_realtime_investment_notification_msg()
        # 如果不存在需要执行的策略
        else:
            return None
    """

if __name__ == '__main__':
    time_start = time.time()
    go = TimeStrategyEquityBondYield()
    #result = go.get_realtime_CSI_300_change()
    #result = go.get_the_lastest_trading_day_CSI_300_yield_rate()
    #result = go.calculate_realtime_equity_bond_yield()
    #result =go.cal_equity_bond_yield_historical_rank(3)
    #result = go.generate_pure_notification_msg()
    #result = go.generate_realtime_investment_notification_msg()
    #go.prepare_index_estimation_bond_rate_and_cal_yield()
    #result = go.main()
    result = go.generate_today_notification_msg()
    #result = go.estimate_realtime_equity_bond_yield()
    #result = go.get_the_lastest_trading_day_CSI_300_yield_ratio()
    #result = go.if_generate_realtime_investment_notification_msg()
    print(result)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))