import time

import sys
sys.path.append('..')
import log.custom_logger as custom_logger
import notification.email_notification as email_notification
import notification.wechat_notification as wechat_notification
import strategy.time_strategy_equity_bond_yield as time_strategy_equity_bond_yield
import strategy.fund_strategy_after_trading_estimation_report as fund_strategy_after_trading_estimation_report

class NotificationPlanAfterTrading:
    # 盘后发送通知计划

    def __init__(self):
        pass

    def equity_bond_yield_strategy_estimation_notification(self):
        # 择时策略, 股债收益率, 邮件通知，微信通知

        # 获取当前时间
        today = time.strftime("%Y-%m-%d", time.localtime())
        # 通知的标题
        title = today+' 股债收益率'

        # 股债收益率
        today_equity_bond_yield_msg = time_strategy_equity_bond_yield.TimeStrategyEquityBondYield().generate_today_notification_msg()
        if today_equity_bond_yield_msg !=None:

            # 邮件发送所有估值信息
            try:
                email_notification.EmailNotification().send_customized_content(title, today_equity_bond_yield_msg)
                # 日志记录
                log_msg = '成功, 成功发送' + today + ' 股债收益率数据至邮件'
                custom_logger.CustomLogger().log_writter(log_msg, 'info')
            except Exception as e:
                # 日志记录
                log_msg = '失败, ' + today + ' 股债收益率数据邮件发送失败 ' + str(e)
                custom_logger.CustomLogger().log_writter(log_msg, 'error')

            # 微信推送所有估值信息
            try:
                wechat_notification.WechatNotification().push_to_all(title, today_equity_bond_yield_msg)
                # 日志记录
                log_msg = '成功, 成功推送' + today + ' 股债收益率数据至微信'
                custom_logger.CustomLogger().log_writter(log_msg, 'info')
            except Exception as e:
                # 日志记录
                log_msg = '失败, ' + today + ' 股债收益率数据微信推送失败 ' + str(e)
                custom_logger.CustomLogger().log_writter(log_msg, 'error')


    def index_fund_strategy_estimation_notification(self):
        # 跟踪标的池中指数基金标的在盘后的估值情况, 邮件通知，微信通知

        # 获取当前时间
        today = time.strftime("%Y-%m-%d", time.localtime())
        # 通知的标题
        title = '标的指数 ' + today + ' 盘后估值'

        # 指数基金标的在盘后的估值情况
        index_fund_estimation_historical_percentage_msg = fund_strategy_after_trading_estimation_report.FundStrategyAfterTradingEstimationReport().generate_msg()

        if index_fund_estimation_historical_percentage_msg !=None:
            # 邮件发送所有估值信息
            try:
                email_notification.EmailNotification().send_customized_content(title, index_fund_estimation_historical_percentage_msg)
                # 日志记录
                log_msg = '成功, 成功发送' + today + ' 股债收益率数据至邮件'
                custom_logger.CustomLogger().log_writter(log_msg, 'info')
            except Exception as e:
                # 日志记录
                log_msg = '失败, ' + today + ' 股债收益率数据邮件发送失败 ' + str(e)
                custom_logger.CustomLogger().log_writter(log_msg, 'error')

            # 微信推送所有估值信息
            try:
                wechat_notification.WechatNotification().push_to_all(title, index_fund_estimation_historical_percentage_msg)
                # 日志记录
                log_msg = '成功, 成功推送' + today + ' 股债收益率数据至微信'
                custom_logger.CustomLogger().log_writter(log_msg, 'info')
            except Exception as e:
                # 日志记录
                log_msg = '失败, ' + today + ' 股债收益率数据微信推送失败 ' + str(e)
                custom_logger.CustomLogger().log_writter(log_msg, 'error')

if __name__ == '__main__':
    time_start = time.time()
    go = NotificationPlanAfterTrading()
    #go.equity_bond_yield_strategy_estimation_notification()
    go.index_fund_strategy_estimation_notification()
    time_end = time.time()
    print(time_end - time_start)