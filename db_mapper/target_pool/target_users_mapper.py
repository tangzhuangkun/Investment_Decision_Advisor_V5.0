#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun



import time
import sys

sys.path.append("..")
import database.db_operator as db_operator

"""
数据表，target_users 的映射
以该表为主的数据操作，均在此完成
"""


class TargetUsersMapper:

    def __init__(self):
        pass

    """
    获取某个推送渠道的全部用户
    :param, channel: 渠道代码，如 email,wechat 等
    :return, 如果存在用户，则返回全部用户的联系ID,为有一个list
            如果不存在，则返回空列表， []
    如，['SCT108958TvJslr4t3t9maVJ1LFGuz2Il3',,,]
    """
    def get_all_channel_users(self,channel):

        # 返回的全部用户的联系ID 列表
        user_list = list()
        # 查询SQL
        selecting_sql = "SELECT contact_id FROM target_users WHERE channel = '%s' " % (channel)
        # 查询
        selecting_result = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        # 将令牌从dict转为list
        for user_unit in selecting_result:
            user_list.append(user_unit['contact_id'])
        return user_list


if __name__ == '__main__':
    time_start = time.time()
    go = TargetUsersMapper()
    result = go.get_all_channel_users("wechat")
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)