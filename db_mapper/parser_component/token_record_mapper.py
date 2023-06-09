#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun



import time
import sys

sys.path.append("..")
import database.db_operator as db_operator

"""
数据表，token_record 的映射
以该表为主的数据操作，均在此完成
"""


class TokenRecordMapper:

    def __init__(self):
        pass

    """
    随机获取某个平台的一个令牌
    :param, platform_code: 平台代码，如 理杏仁的代码为 lxr
    :return , token
    a3bd70c2-7e3c-4661-b2cb-38342b222be5
    """
    def get_one_token(self,platform_code):
        # 查询SQL
        selecting_sql = """SELECT token FROM token_record WHERE platform_code = '%s' ORDER BY RAND() LIMIT 1 """ % (platform_code)
        # 查询
        selecting_result = db_operator.DBOperator().select_one("parser_component", selecting_sql)
        return selecting_result["token"]


if __name__ == '__main__':
    time_start = time.time()
    go = TokenRecordMapper()
    result = go.get_one_token("lxr")
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)