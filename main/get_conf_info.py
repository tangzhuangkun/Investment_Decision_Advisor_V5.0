#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import random
import sys

sys.path.append("..")
import conf

class GetConfInfo:
    # 获取运行程序所需的配置信息

    def __init__(self):
        pass

    def get_lxr_token(self):
        # 随机获取一个理杏仁账号的token
        token_len = len(conf.token_list)
        random_num = random.randint(0, token_len-1)
        return conf.token_list[random_num]


if __name__ == '__main__':
    go = GetConfInfo()
    result = go.get_lxr_token()
    print(result)