import time

import sys

sys.path.append('..')
import strategy.index_strategy_PB_estimation as index_strategy_PB_estimation
import strategy.index_strategy_PE_estimation as index_strategy_PE_estimation
import strategy.stock_strategy_monitoring_estimation as stock_strategy_monitoring_estimation
import log.custom_logger as custom_logger
import notification.email_notification as email_notification
import notification.wechat_notification as wechat_notification
import strategy.time_strategy_equity_bond_yield as time_strategy_equity_bond_yield


class NotificationPlanDuringTrading:
    # 盘中发送通知计划

    def __init__(self):
        pass

    def daily_estimation_notification(self):
        # 指数估值信息, 邮件通知，微信通知，只要针对 指数
        # 频率：日级

        # 计算指数的动态市盈率
        indexes_and_real_time_PE_msg = index_strategy_PE_estimation.IndexStrategyPEEstimation().main()
        # 计算指数的市净率
        indexes_and_real_time_PB_msg = index_strategy_PB_estimation.IndexStrategyPBEstimation().main()

        # 估值信息汇总
        estimation_msg = indexes_and_real_time_PE_msg + '\n\n' + indexes_and_real_time_PB_msg

        # 获取当前时间
        today = time.strftime("%Y-%m-%d", time.localtime())

        # 邮件发送所有估值信息
        try:
            email_notification.EmailNotification().send_customized_content(today + ' 指数基金估值数据', estimation_msg)
            # 日志记录
            log_msg = '成功, 成功发送' + today + '指数基金估值数据至邮件'
            custom_logger.CustomLogger().log_writter(log_msg, 'info')
        except Exception as e:
            # 日志记录
            log_msg = '失败, ' + today + '指数基金估值数据邮件发送失败 ' + str(e)
            custom_logger.CustomLogger().log_writter(log_msg, 'error')

        # 微信推送所有估值信息
        try:
            wechat_notification.WechatNotification().push_to_all(today + ' 指数基金估值数据', estimation_msg)
            # 日志记录
            log_msg = '成功, 成功推送' + today + '指数基金估值数据至微信'
            custom_logger.CustomLogger().log_writter(log_msg, 'info')
        except Exception as e:
            # 日志记录
            log_msg = '失败, ' + today + '指数基金估值数据微信推送失败 ' + str(e)
            custom_logger.CustomLogger().log_writter(log_msg, 'error')

    def minutely_stock_estimation_notification(self):
        # 估值信息, 邮件通知，微信通知，只要针对 股票
        # 频率：分钟级

        # 计算股票的预设条件触发信息
        # 如 {'000002': [('sz000002', '万科A', 'pb', '0.98', '小于设定估值 0.99'), ('sz000002', '万科A', 'pe_ttm', '5.84', '3.39% 小于设定估值百分位 12%')],
        # '600048': [('sh600048', '保利发展', 'pb', '1.07', '小于设定估值 1.1')]}
        triggered_stocks_info_dict = stock_strategy_monitoring_estimation.StockStrategyMonitoringEstimation().main()
        if (len(triggered_stocks_info_dict) != 0):

            # 当前分钟
            current_minute = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))

            for stock_code in triggered_stocks_info_dict:
                # 股票带上市地代码
                stock_code_with_location = triggered_stocks_info_dict.get(stock_code)[0][0]
                # 股票名称
                stock_name = triggered_stocks_info_dict.get(stock_code)[0][1]
                # 返回的信息列表
                # [('sz000002', '万科A', 'pb', '0.98', '小于设定估值 0.99'), ('sz000002', '万科A', 'pe_ttm', '5.84', '3.39% 小于设定估值百分位 12%')]
                return_msg_list = triggered_stocks_info_dict.get(stock_code)
                # 编辑推送的信息
                trigger_msg = ""
                trigger_msg += "股票名称： " + stock_name + "\n"
                trigger_msg += "股票代码： " + stock_code_with_location + "\n"
                for each_msg in return_msg_list:
                    # 拼接 策略方法+实时估值情况+被推送的原因
                    trigger_msg += each_msg[2] + ": " + str(each_msg[3]) + "\n"
                    trigger_msg += each_msg[4] + "\n\n"
                # 邮件发送所有触发信息
                try:
                    email_notification.EmailNotification().send_customized_content(
                        stock_name + " 于 " + current_minute + '的触发信息', trigger_msg)
                    # 日志记录
                    log_msg = '成功, 成功发送' + current_minute + stock_name + ' 股票的触发信息至邮件'
                    custom_logger.CustomLogger().log_writter(log_msg, 'info')
                except Exception as e:
                    # 日志记录
                    log_msg = '失败, ' + current_minute + stock_name + ' 股票的触发信息至邮件失败 ' + str(e)
                    custom_logger.CustomLogger().log_writter(log_msg, 'error')

                # 微信推送所有估值信息
                try:
                    wechat_notification.WechatNotification().push_to_all(
                        stock_name + " 于 " + current_minute + '的触发信息', trigger_msg)
                    # 日志记录
                    log_msg = '成功, 成功发送' + current_minute + stock_name + ' 股票的触发信息至微信'
                    custom_logger.CustomLogger().log_writter(log_msg, 'info')
                except Exception as e:
                    # 日志记录
                    log_msg = '失败, ' + current_minute + stock_name + '股票的触发信息微信推送失败 ' + str(e)
                    custom_logger.CustomLogger().log_writter(log_msg, 'error')

    def minutely_equity_bond_yield_notification(self):
        # 通知 预估的实时股债收益比信息, 邮件通知，微信通知，目前只针对 股债收益比
        # 频率：分钟级

        # 当前时间
        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        # 通知的标题
        title = current_time + ' 预估实时股债收益率触发阈值'
        # 预估的实时股债收益比信息
        estimated_realtime_equity_bond_yield_msg = time_strategy_equity_bond_yield.TimeStrategyEquityBondYield().if_generate_realtime_investment_notification_msg()
        # 如果有返回信息
        if (estimated_realtime_equity_bond_yield_msg != None):

            # 邮件发送所有估值信息
            try:
                email_notification.EmailNotification().send_customized_content(title,
                                                                               estimated_realtime_equity_bond_yield_msg)
                # 日志记录
                log_msg = '成功, 成功发送' + current_time + ' 的预估实时股债收益率数据至邮件'
                custom_logger.CustomLogger().log_writter(log_msg, 'info')
            except Exception as e:
                # 日志记录
                log_msg = '失败, ' + current_time + ' 的预估实时股债收益率数据邮件发送失败 ' + str(e)
                custom_logger.CustomLogger().log_writter(log_msg, 'error')

            # 微信推送所有估值信息
            try:
                wechat_notification.WechatNotification().push_to_all(title, estimated_realtime_equity_bond_yield_msg)
                # 日志记录
                log_msg = '成功, 成功推送' + current_time + ' 的预估实时股债收益率数据至微信'
                custom_logger.CustomLogger().log_writter(log_msg, 'info')
            except Exception as e:
                # 日志记录
                log_msg = '失败, ' + current_time + ' 的预估实时股债收益率数据微信推送失败 ' + str(e)
                custom_logger.CustomLogger().log_writter(log_msg, 'error')


if __name__ == '__main__':
    time_start = time.time()
    go = NotificationPlanDuringTrading()
    go.daily_estimation_notification()
    # go.minutely_stock_estimation_notification()
    # go.minutely_equity_bond_yield_notification()
    time_end = time.time()
    print(time_end - time_start)
