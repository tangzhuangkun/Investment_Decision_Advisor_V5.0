#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import requests
import re
import random
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import sys
sys.path.append("..")
import parsers.disguise as disguise
import conf


"""
获取中证指数网站cookie
参考来源： https://www.freesion.com/article/76691150891/
"""


class GetCSIndexCookie:

    def __init__(self, url, header, proxy):
        # 请求接口
        self.url = url
        self.header = header
        self.proxy = proxy
        self.timeout_limit = 5

    def get_hexxor(self, s1, _0x4e08d8):
        _0x5a5d3b = ''

        for i in range(len(s1)):
            if i % 2 != 0: continue
            _0x401af1 = int(s1[i: i+2], 16)
            _0x105f59 = int(_0x4e08d8[i: i+2], 16)
            _0x189e2c_10 = (_0x401af1 ^ _0x105f59)
            _0x189e2c = hex(_0x189e2c_10)[2:]
            if len(_0x189e2c) == 1:
                _0x189e2c = '0' + _0x189e2c
            _0x5a5d3b += _0x189e2c
        return _0x5a5d3b

    def get_unsbox(self, arg1):
        _0x4b082b = [0xf, 0x23, 0x1d, 0x18, 0x21, 0x10, 0x1, 0x26, 0xa, 0x9, 0x13, 0x1f, 0x28, 0x1b, 0x16, 0x17, 0x19, 0xd,
                     0x6, 0xb, 0x27, 0x12, 0x14, 0x8, 0xe, 0x15, 0x20, 0x1a, 0x2, 0x1e, 0x7, 0x4, 0x11, 0x5, 0x3, 0x1c,
                     0x22, 0x25, 0xc, 0x24]
        _0x4da0dc = []
        _0x12605e = ''
        for i in _0x4b082b:
            _0x4da0dc.append(arg1[i-1])
        _0x12605e = "".join(_0x4da0dc)
        return _0x12605e

    def get_cookie(self):
        # 第一次请求获取js代码
        #headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36"}
        cookie = dict()

        r = requests.get(self.url, headers=self.header, proxies=self.proxy, verify=False, stream=False,
                                    timeout=self.timeout_limit)
        # 重js中匹配出 arg1
        arg1 = re.findall("arg1=\'(.*?)\'", r.text)[0]

        # 参数生成
        s1 = self.get_unsbox(arg1)
        _0x4e08d8 = "3000176000856006061501533003690027800375"
        _0x12605e = self.get_hexxor(s1, _0x4e08d8)
        #print(s1, _0x12605e)
        # 二次请求携带cookie 获取html文件
        self.header["cookie"] = "acw_sc__v2="+_0x12605e
        cookie["cookie"] = "acw_sc__v2=" + _0x12605e
        return cookie

if __name__ == '__main__':

    ip_address_dict_list, ua_dict_list = disguise.Disguise().get_multi_IP_UA(1)
    # 随机选取，伪装，隐藏UA和IP
    pick_an_int = random.randint(0, 0)
    # header = {"user-agent": self.ua_dict_list[random.randint(0, self.IP_UA_num*5 - 1)]['ua'], 'Connection': 'keep-alive'}
    header = {"user-agent": ua_dict_list[random.randint(0, 1 * 5 - 1)]['ua'], 'Connection': 'close'}
    proxy = {"http": 'http://{}:{}@{}'.format(conf.proxyIPUsername, conf.proxyIPPassword,
                                              ip_address_dict_list[pick_an_int]['ip_address']),
             "https": 'https://{}:{}@{}'.format(conf.proxyIPUsername, conf.proxyIPPassword,
                                                ip_address_dict_list[pick_an_int]['ip_address'])}


    url = "https://www.csindex.com.cn/csindex-home/perf/get-index-yield-item-nianHua/930955"
    go = GetCSIndexCookie(url, header, proxy)
    cookie = go.get_cookie()
    print(cookie)
