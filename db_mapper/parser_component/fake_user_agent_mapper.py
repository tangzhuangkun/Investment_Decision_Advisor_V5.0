#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun



import time
import sys

sys.path.append("..")
import database.db_operator as db_operator

"""
数据表，fake_user_agent 的映射
以该表为主的数据操作，均在此完成
"""


class FakeUserAgentMapper:

    def __init__(self):
        pass

    """
    获取X个UA
    :param， num， 需要获取多少个UA
    """
    def get_ua(self, num=1):
        # 获取UA
        ua_sql = "SELECT ua FROM parser_component.fake_user_agent ORDER BY RAND() LIMIT %s" %(num)
        ua = db_operator.DBOperator().select_all('parser_component', ua_sql)
        return ua



if __name__ == '__main__':
    time_start = time.time()
    go = FakeUserAgentMapper()
    result = go.get_ua(5)
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)