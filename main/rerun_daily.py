#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import time

import sys
sys.path.append('..')
import log.custom_logger as custom_logger
import parsers.generate_save_user_agent as generate_save_user_agent
import data_collector.collect_stock_historical_estimation_info as collect_stock_historical_estimation_info
import notification.notification_plan_during_trading as notification_plan_during_trading
import notification.notification_plan_after_trading as notification_plan_after_trading
import data_collector.collect_trading_days as collect_trading_days
import data_miner.calculate_index_historial_estimations as calculate_index_historial_estimations
import data_collector.collect_csindex_top_10_stocks_weight_daily as collect_csindex_top_10_stocks_weight_daily
import data_collector.collect_index_weight_from_csindex_file as collect_index_weight_from_csindex_file
import data_collector.collect_index_weight_from_cnindex_interface as collect_index_weight_from_cnindex_interface
import data_miner.gather_all_tracking_stocks as gather_all_tracking_stocks
import web_service.web_service_impl as web_service_impl
import data_collector.collect_chn_gov_bonds_rates as collect_chn_gov_bonds_rates
import data_collector.collect_index_estimation_from_lxr as collect_index_estimation_from_lxr
import data_miner.calculate_stock_bond_ratio as calculate_stock_bond_ratio
import data_collector.collect_excellent_index_from_cs_index as collect_excellent_index_from_cs_index
import data_collector.collect_excellent_index_from_cn_index as collect_excellent_index_from_cn_index


"""
补数专用-日级
"""
class RerunDaily:
    def __init__(self):
        pass

    def main(self):

        # ------------ 盘中 ----------------
        # 每分钟执行一次股票的监控策略
        # notification_plan_during_trading.NotificationPlanDuringTrading().minutely_stock_estimation_notification()
        # print("每分钟执行一次股票的监控策略, 完成")
        #
        # # 盘中计算并通过邮件/微信发送指数的动态估值信息
        # notification_plan_during_trading.NotificationPlanDuringTrading().daily_estimation_notification()
        # print("盘中计算并通过邮件/微信发送指数的动态估值信息, 完成")

        # ------------ 盘后 ----------------
        # 收集交易日信息
        collect_trading_days.CollectTradingDays().main()
        print("收集交易日信息, 完成")

        # 收集中证官网指数的最新构成信息
        collect_index_weight_from_csindex_file.CollectIndexWeightFromCSIndexFile().main()
        print("收集中证官网指数的最新构成信息, 完成")

        # 收集中证官网指数前十权重股的最新构成信息
        collect_csindex_top_10_stocks_weight_daily.CollectCSIndexTop10StocksWeightDaily().main()
        print("收集中证官网指数前十权重股的最新构成信息, 完成")

        # 收集国证官网指数最新构成信息
        collect_index_weight_from_cnindex_interface.CollectIndexWeightFromCNIndexInterface().main()
        print("收集国证官网指数最新构成信息, 完成")

        # 聚合汇总所有需要被跟踪的股票
        gather_all_tracking_stocks.GatherAllTrackingStocks().main()
        print("聚合汇总所有需要被跟踪的股票, 完成")

        # 收集所需的股票的估值信息
        #collect_stock_historical_estimation_info.CollectStockHistoricalEstimationInfo().main()
        #print("收集所需的股票的估值信息, 完成")

        # 计算指数估值
        # calculate_index_historial_estimations.CalculateIndexHistoricalEstimations().main()
        # print("计算指数估值, 完成")

        # 通过邮件/微信发送标的股票估值报告
        #notification_plan_after_trading.NotificationPlanAfterTrading().stock_strategy_estimation_notification()
        #print("通过邮件/微信发送标的股票估值报告, 完成")

        # 收集最新国债收益率
        #collect_chn_gov_bonds_rates.CollectCHNGovBondsRates().main()
        #print("收集最新国债收益率, 完成")

        # 收集最新沪深300指数市值加权估值
        #collect_index_estimation_from_lxr.CollectIndexEstimationFromLXR().main()
        #print("收集最新沪深300指数市值加权估值, 完成")

        # 运行mysql脚本，计算股债收益率
        #calculate_stock_bond_ratio.CalculateStockBondRatio().main()
        #print("运行mysql脚本，计算股债收益率, 完成")

        # 收集沪深300指数/沪深A股估值，国债收益，计算并通过邮件/微信发送股当日债收益比
        #notification_plan_after_trading.NotificationPlanAfterTrading().stock_bond_yield_strategy_estimation_notification()
        #print("收集沪深300指数/沪深A股估值，国债收益，计算并通过邮件/微信发送股当日债收益比, 完成")

        # 通过邮件/微信发送标的指数估值报告
        #notification_plan_after_trading.NotificationPlanAfterTrading().index_strategy_estimation_notification()
        #print("通过邮件/微信发送标的指数估值报告, 完成")

        # 将所有暂停标的策略重新开启，下一个交易日又可生效
        #web_service_impl.WebServericeImpl().restart_all_mute_target()
        #print("将所有暂停标的策略重新开启，下一个交易日又可生效, 完成")

if __name__ == '__main__':
    time_start = time.time()
    go = RerunDaily()
    go.main()
    time_end = time.time()
    print('time:')
    print(time_end - time_start)