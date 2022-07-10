#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import sys
import time
import requests

sys.path.append("..")
import log.custom_logger as custom_logger
import data_miner.data_miner_common_db_operator as data_miner_common_db_operator

class WechatNotification:
    # 发送微信通知

    def __init__(self):
        pass

    def replace_one_enter_key_with_two(self, send_content):
        '''
        # 匹配server酱（方糖）发送消息内容的规则，在markdown中，两次回车才是换行
        # 将原本需要发送的文本中，每个换行符都替换成两个，才能真正在接受到的消息中看到换行符
        :param send_content: , 自定义的内容
        :return: send_content中的每个回车符号替换成两个
        '''
        replaced_send_content = send_content.replace("\n\n","\n\n***********\n\n")
        replaced_send_content = replaced_send_content.replace("\n","\n\n")
        return replaced_send_content

    def push_customized_content(self, token, object, send_content):
        # 自定义微信推送主题+内容，仅推送给特定的人
        # param: token, 收信人token
        # param: object, 主题
        # param: send_content, 自定义的内容
        # 发送微信推送

        try:
            # 调用接口发送推送
            # server酱（方糖）接口
            url = "https://sctapi.ftqq.com/"+token+".send?title=" + object + "&desp=" + send_content
            requests.post(url)
            # 日志记录
            log_msg = "成功, 向" + token +" "+object+" "+send_content+ " 微信推送成功"
            custom_logger.CustomLogger().log_writter(log_msg, 'info')
        except Exception as e:
            # 日志记录
            log_msg = "失败, 微信推送失败" + object + "  " + send_content + str(e)
            custom_logger.CustomLogger().log_writter(log_msg, 'error')



    def push_to_all(self, object, send_content):
        # 将自定义内容微信推送给所有人
        # param: object, 邮件主题
        # param: send_content, 自定义的内容

        # 获取token，决定需要推送给哪些人
        tokens = data_miner_common_db_operator.DataMinerCommonDBOperator().get_all_tokens("ServerChan")
        # 需要发送的文本中，每个换行符都替换成两个
        replaced_send_content = self.replace_one_enter_key_with_two(send_content)
        # 推送给所有人
        for token in tokens:
            self.push_customized_content(token, object, replaced_send_content)


if __name__ == '__main__':
    time_start = time.time()
    go = WechatNotification()
    # 获取当前时间
    today = time.strftime("%Y-%m-%d", time.localtime())
    go.push_to_all(' 基金行情分析test', '测试从数据库获取token')
    time_end = time.time()
    print(time_end-time_start)