#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

from apscheduler.schedulers.blocking import BlockingScheduler
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

class Scheduler:
	# 任务调度器，根据时间安排工作
	def __init__(self):
		pass


	def schedule_plan(self):
		# 调度器，根据时间安排工作
		scheduler = BlockingScheduler()

		#####################      每天运行    ###################################################

		#########  盘前(00:00-9:29)  #########
		try:
			# 每天创建一个日级日志文件
			scheduler.add_job(func=custom_logger.CustomLogger().daily_log_creator,
							  trigger='cron',hour=1, minute=1, day_of_week='mon,tue,wed,thu,fri',
							  id='createDailyLogFile')

		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')


		#########  盘中(9:30-15:00)  #########

		try:
			# 每分钟执行一次股票的监控策略
			scheduler.add_job(func=notification_plan_during_trading.NotificationPlanDuringTrading().minutely_stock_estimation_notification,
							  trigger='cron',
							  hour='9,10,11,13,14,15,16',minute='0-59',day_of_week='mon,tue,wed,thu,fri',
							  id='tradingdaymonitorstocksestimationsandtriggers')
		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')


		try:
			# 每分钟执行一次股债收益比的监控策略
			scheduler.add_job(func=notification_plan_during_trading.NotificationPlanDuringTrading().minutely_equity_bond_yield_notification,
							  trigger='cron',
							  hour='9,10,11,13,14,15,16',minute='0-59',second='30',day_of_week='mon,tue,wed,thu,fri',
							  id='tradingdaymonitorequitybondyield')
		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')


		try:
			# 每个交易日14：49计算并通过邮件/微信发送指数的动态估值信息
			scheduler.add_job(func=notification_plan_during_trading.NotificationPlanDuringTrading().
							  daily_estimation_notification,
							  trigger='cron',
							  month='1-12', day_of_week='mon,tue,wed,thu,fri', hour=14, minute=49,
							  id='weekdayDuringTradingEstimationNotification')
		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')


		try:
			# 每个交易日14：50执行一次股债收益比的监控策略
			scheduler.add_job(func=notification_plan_during_trading.NotificationPlanDuringTrading().minutely_equity_bond_yield_notification,
							  trigger='cron',
							  month='1-12', day_of_week='mon,tue,wed,thu,fri', hour=14, minute=50,
							  id='tradingDayMonitorEquityBondYieldTwo')
		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')


		#########  盘后(15:00-23:59)  #########
		try:
			# 每个交易日18：00 聚合汇总所有需要被跟踪的股票
			scheduler.add_job(func=gather_all_tracking_stocks.GatherAllTrackingStocks().main,
							  trigger='cron',
							  month='1-12', day_of_week='mon,tue,wed,thu,fri', hour=18,
							  id='gatherAllTrackingStocks')
		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')


		try:
			# 每个交易日18：01收集交易日信息
			scheduler.add_job(func=collect_trading_days.CollectTradingDays().main,
							  trigger='cron',
							  month='1-12', day_of_week='mon,tue,wed,thu,fri', hour=18, minute=1,
							  id='weekdayCollectTradingDays')
		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')

		try:
			# 每个交易日18：02收集中证官网指数的最新构成信息
			scheduler.add_job(func=collect_index_weight_from_csindex_file.CollectIndexWeightFromCSIndexFile().main,
							  trigger='cron',
							  month='1-12', day_of_week='mon,tue,wed,thu,fri', hour=18, minute=2,
							  id='weekdayCollectCSIndexStocksWeightFromFile')
		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')

		try:
			# 每个交易日18：03收集中证官网指数前十权重股的最新构成信息
			scheduler.add_job(func=collect_csindex_top_10_stocks_weight_daily.CollectCSIndexTop10StocksWeightDaily().main,
							  trigger='cron',
							  month='1-12', day_of_week='mon,tue,wed,thu,fri', hour=18, minute=3,
							  id='weekdayCollectCSIndexTop10StocksWeight')
		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')

		try:
			# 每个交易日18：04收集国证官网指数最新构成信息
			scheduler.add_job(func=collect_index_weight_from_cnindex_interface.CollectIndexWeightFromCNIndexInterface().main,
							  trigger='cron',
							  month='1-12', day_of_week='mon,tue,wed,thu,fri', hour=18, minute=4,
							  id='weekdayCollectCNIndexStocksWeightFromInterface')
		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')

		try:
			# 每个交易日18：05收集所需的股票的估值信息
			scheduler.add_job(func=collect_stock_historical_estimation_info.CollectStockHistoricalEstimationInfo().main,
							  trigger='cron',
							  month='1-12', day_of_week='mon,tue,wed,thu,fri', hour=18, minute=5,
							  id='weekdayCollectStockHistoricalEstimationInfo')
		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')

		try:
			# 每个交易日18：08计算指数估值
			scheduler.add_job(func=calculate_index_historial_estimations.CalculateIndexHistoricalEstimations().main,
							  trigger='cron',
							  month='1-12', day_of_week='mon,tue,wed,thu,fri', hour=18, minute=8,
							  id='weekdayCalculateIndexHistoricalEstimations')
		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')

		try:
			# 每个交易日18:10:30通过邮件/微信发送标的股票估值报告
			scheduler.add_job(func=notification_plan_after_trading.NotificationPlanAfterTrading().stock_strategy_estimation_notification,
							  trigger='cron',
							  month='1-12', day_of_week='mon,tue,wed,thu,fri', hour=18, minute=10, second=30,
							  id='weekdayTargetStockEstimationReport')
		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')

		try:
			# 每个交易日18：15，收集沪深300指数/沪深A股估值，国债收益，计算并通过邮件/微信发送股当日债收益比
			scheduler.add_job(func=notification_plan_after_trading.NotificationPlanAfterTrading().equity_bond_yield_strategy_estimation_notification,
							  trigger='cron',
							  month='1-12', day_of_week='mon,tue,wed,thu,fri', hour=18, minute=15,
							  id='weekdayAfterTradingNotification')
		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')

		try:
			# 每个交易日18：17通过邮件/微信发送标的指数估值报告
			scheduler.add_job(func=notification_plan_after_trading.NotificationPlanAfterTrading().index_strategy_estimation_notification,
							  trigger='cron',
							  month='1-12', day_of_week='mon,tue,wed,thu,fri', hour=18, minute=17,
							  id='weekdayTargetIndexEstimationReport')
		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')

		try:
			# 每个交易日19：01, 将所有暂停标的策略重新开启，下一个交易日又可生效
			scheduler.add_job(func=web_service_impl.WebServericeImpl().restart_all_mute_target,
							  trigger='cron',
							  month='1-12', day_of_week='mon,tue,wed,thu,fri', hour=19, minute=1,
							  id='weekdayRestartAllMuteTarget')
		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')


		#####################      每周运行    ###################################################

		try:
			# 每个星期天晚上23:00重新生成一批假的user_agent
			scheduler.add_job(func=generate_save_user_agent.GenerateSaveUserAgent().main, trigger='cron',
							  month='1-12', day_of_week='sun', hour=23,
							  id='sundayGenerateFakeUserAgent')
		except Exception as e:
			# 抛错
			custom_logger.CustomLogger().log_writter(e, 'error')

		#####################      每月运行    ###################################################


		# 启动调度器
		try:
			scheduler.start()
		except (KeyboardInterrupt, SystemExit):
			pass




if __name__ == "__main__":
	go = Scheduler()
	go.schedule_plan()








