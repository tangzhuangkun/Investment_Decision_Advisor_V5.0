import time

import sys
sys.path.append('..')
import log.custom_logger as custom_logger
import notification.email_notification as email_notification
import notification.wechat_notification as wechat_notification
import strategy.time_strategy_equity_bond_yield as time_strategy_equity_bond_yield

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
        time_strategy_msg = time_strategy_equity_bond_yield.TimeStrategyEquityBondYield().main()
        if ("投资" in time_strategy_msg):
            title = ' 股债收益率, 已进入投资区间'

        # 邮件发送所有估值信息
        try:
            email_notification.EmailNotification().send_customized_content(title, time_strategy_msg)
            # 日志记录
            log_msg = '成功, 成功发送' + today + ' 股债收益率数据至邮件'
            custom_logger.CustomLogger().log_writter(log_msg, 'info')
        except Exception as e:
            # 日志记录
            log_msg = '失败, ' + today + ' 股债收益率数据邮件发送失败 ' + str(e)
            custom_logger.CustomLogger().log_writter(log_msg, 'error')

        # 微信推送所有估值信息
        try:
            wechat_notification.WechatNotification().push_to_all(title, time_strategy_msg)
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
    go.equity_bond_yield_strategy_estimation_notification()
    time_end = time.time()
    print(time_end - time_start)