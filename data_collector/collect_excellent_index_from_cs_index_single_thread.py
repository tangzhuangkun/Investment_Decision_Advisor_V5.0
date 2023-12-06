#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import time
import requests
import json
import threading
import random
import decimal

import sys
sys.path.append("..")
import parsers.disguise as disguise
import log.custom_logger as custom_logger
import conf
import db_mapper.financial_data.index_excellent_performance_indices_di_mapper as index_excellent_performance_indices_di_mapper
import db_mapper.financial_data.fin_data_total_return_indexes_list_mapper as fin_data_total_return_indexes_list_mapper

"""
从中证指数官网接口收集过去几年表现优异的指数
3年年化收益率 或 5年年化收益率 满足要求
均存入数据库
"""

class CollectExcellentIndexFromCSIndexSingleThread:

    def __init__(self):

        # 衡量标准
        # 3年年化收益率
        self.three_year_yield_rate_standard = 5
        # 5年年化收益率
        self.five_year_yield_rate_standard = 5
        # 最大线程数
        self.max_thread_num = 15
        # 同时获取x个IP和5x个UA
        self.IP_UA_num = 3
        # 将中证所有指数代码分成多个区块，每个区块最多拥有多少个指数代码
        self.max_index_codes = 20
        # 每个区块执行的时间
        self.sleep_time = 7
        # 链接超时时间限制
        self.timeout_limit = 5
        # 指数及其对应的全收益指数信息
        self.index_relate_tr_dict = dict()
        # 获取指数表现
        self.index_yield_url = "https://www.csindex.com.cn/csindex-home/perf/get-index-yield-item/"


    """
    从中证接口获取全部指数的代码和名称
    :param header: 伪装的UA
    :param proxy: 伪装的IP
    :return,  指数代码列表, [ (指数代码, 全收益指数代码)]
    如 [('000979', 'H00979'), ('000984', 'H00984'), ('930927', None), ,,,]
    """
    def parse_interface_to_get_index_code_name_content(self, header, proxy):

        # 指数代码
        index_code_list= list()

        # 接口地址
        interface_url = "https://www.csindex.com.cn/csindex-home/index-list/query-index-item"
        # 处理python中无null的问题
        null = None
        # 传入的参数, 是否有跟踪产品选是（1）
        body = {"sorter": {"sortField": "null", "sortOrder": null},
                "pager": {"pageNum": 1, "pageSize": 10000},
                "indexFilter": {"ifCustomized": null, "ifTracked": 1, "ifWeightCapped": null,
                                "indexCompliance": null,
                                "hotSpot": null, "indexClassify": null, "currency": null, "region": null,
                                "indexSeries": null,
                                "undefined": null}
                }

        # 递归算法，处理异常
        try:
            # 增加连接重试次数,默认10次
            requests.adapters.DEFAULT_RETRIES = 10
            # 关闭多余的连接：requests使用了urllib3库，默认的http connection是close的，
            # requests设置False关闭
            s = requests.session()
            s.keep_alive = False

            # 忽略警告
            requests.packages.urllib3.disable_warnings()
            # 得到页面的信息
            raw_page = requests.post(interface_url, headers=header, proxies=proxy, verify=False, stream=False,json=body,
                                    timeout=self.timeout_limit).text
            # 转换成字典数据
            data_json = json.loads(raw_page)["data"]

            # 存入列表中
            for index_info in data_json:
                index_code = index_info["indexCode"]
                # 如果该指数存在关联的全收益指数
                if index_code in self.index_relate_tr_dict:
                # 全收益指数代码
                    index_code_tr = self.index_relate_tr_dict.get(index_code).get('index_code_tr')
                    # 将指数代码和全收益指数代码组装成一个pair
                    index_code_list.append((index_code, index_code_tr))
                else:
                    index_code_list.append((index_code, None))
            return index_code_list

        except Exception as e:
            # 日志记录
            msg = "从中证官网接口 " + interface_url + '  ' + "获取全部指数的代码和名称失败 " + str(e) + " 即将重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.call_interface_to_get_all_index_code_name_from_cs_index()


    """
    调用接口，从中证指数官网获取全部指数代码和名称
    :return,  指数代码列表, [ (指数代码, 全收益指数代码)]
    如 [('000979', 'H00979'), ('000984', 'H00984'), ('930927', None), ,,,]
    """
    def call_interface_to_get_all_index_code_name_from_cs_index(self):

        # 此处获取全部指数代码和名称可能存在 超时，链接出错等原因导致 获取到信息的时间过长，约20秒-40秒
        # 代理IP存活时长一般为1分钟左右，若一口气调用太多代理IP，大概率存在代理IP已失活
        # 此处为该脚本第一次调用 代理IP的API，且存在获取时间过长，因此仅调用1个IP和1个UA
        # 伪装，隐藏UA和IP
        header, proxy = disguise.Disguise().assemble_header_proxy()

        return self.parse_interface_to_get_index_code_name_content(header, proxy)


    """
    获取指数的收益表现
    :param index_code: 指数代码（6位数字， 如 399997）
    :param index_performance_dict, 指数收益表现的字典
    :param header: 伪装的UA
    :param proxy: 伪装的IP
    """
    def parse_index_yield_performance(self, index_code, index_performance_dict, header, proxy):

        # 接口地址
        interface_url = self.index_yield_url + index_code

        # 递归算法，处理异常
        try:
            # 增加连接重试次数,默认10次
            requests.adapters.DEFAULT_RETRIES = 10
            # 关闭多余的连接：requests使用了urllib3库，默认的http connection是close的，
            # requests设置False关闭
            s = requests.session()
            s.keep_alive = False

            # 忽略警告
            requests.packages.urllib3.disable_warnings()
            # 得到页面的信息
            raw_page = requests.get(interface_url, headers=header, proxies=proxy, verify=False, stream=False,
                                    timeout=self.timeout_limit).text

            # 转换成字典数据
            raw_data = json.loads(raw_page)
            # 获取data数据
            data_json = raw_data["data"]

            # 指数代码和名称
            index_name = data_json["indexNameCn"]
            index_performance_dict["index_code_re"] = index_code
            index_performance_dict["index_name_re"] = index_name


            # 获取接口中指数各阶段收益率情况
            one_month_yield_rate_re = 0
            three_month_yield_rate_re = 0
            this_year_yield_rate_re = 0
            one_year_yield_rate_re = 0
            three_year_yield_rate_re = 0
            five_year_yield_rate_re = 0

            if (data_json["oneMonth"] != None):
                one_month_yield_rate_re = float(data_json["oneMonth"])
            index_performance_dict["one_month_yield_rate_re"] = one_month_yield_rate_re
            if (data_json["threeMonth"] != None):
                three_month_yield_rate_re = float(data_json["threeMonth"])
            index_performance_dict["three_month_yield_rate_re"] = three_month_yield_rate_re
            if (data_json["thisYear"] != None):
                this_year_yield_rate_re = float(data_json["thisYear"])
            index_performance_dict["this_year_yield_rate_re"] = this_year_yield_rate_re
            if (data_json["oneYear"] != None):
                one_year_yield_rate_re = float(data_json["oneYear"])
            index_performance_dict["one_year_yield_rate_re"] = one_year_yield_rate_re
            if (data_json["threeYear"] != None):
                three_year_yield_rate_re = float(data_json["threeYear"])
            index_performance_dict["three_year_yield_rate_re"] = three_year_yield_rate_re
            if (data_json["fiveYear"] != None):
                five_year_yield_rate_re = float(data_json["fiveYear"])
            index_performance_dict["five_year_yield_rate_re"] = five_year_yield_rate_re

            return index_performance_dict

        except Exception as e:
            # 日志记录
            msg = "从中证官网接口" + interface_url + '  ' + "获取指数过去表现 " + str(e) + " 即将重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.parse_index_yield_performance(index_code, index_performance_dict, header, proxy)


    def parse_and_check_whether_an_excellent_index(self,index_code_pair, header, proxy):
        '''
        解析判断是否为符合要求的优秀指数
        解析接口内容，获取单个指数过去几年的表现
        :param index_code_pair: 指数代码pair(指数代码, 全收益指数代码), 如 ('000979', 'H00979'), ('930927', None)
        :param satisfied_index_list: 满足收集指标的指数
        :param threadLock: 线程锁
        :param: same_time_threading, 线程数量的限制
        :param header: 伪装的UA
        :param proxy: 伪装的IP
        :return:
        '''

        # 指数收益表现的字典
        index_performance_dict = dict()

        # 常规指数代码
        index_code_re = index_code_pair[0]
        # 全收益指数代码
        index_code_tr = index_code_pair[1]

        # 默认使用常规指数代码
        index_code = index_code_pair[0]
        index_performance_dict["type"] = "regular_index"

        # 如果该指数存在全收益率代码，则使用全收益率代码
        if index_code_pair[1] != None:
            index_code = index_code_pair[1]
            index_performance_dict["type"] = "total_return_index"


        # 接口地址
        interface_url = self.index_yield_url + index_code

        # 递归算法，处理异常
        try:
            # 增加连接重试次数,默认10次
            requests.adapters.DEFAULT_RETRIES = 10
            # 关闭多余的连接：requests使用了urllib3库，默认的http connection是close的，
            # requests设置False关闭
            s = requests.session()
            s.keep_alive = False

            # 忽略警告
            requests.packages.urllib3.disable_warnings()
            # 得到页面的信息
            raw_page = requests.get(interface_url, headers=header, proxies=proxy, verify=False, stream=False,
                                    timeout=self.timeout_limit ).text

            # 转换成字典数据
            raw_data = json.loads(raw_page)
            # 判断接口是否调用成功
            if (not raw_data["success"]):
                #same_time_threading.release()
                return

            # 获取data数据
            data_json = raw_data["data"]
            if(data_json==None):
                #same_time_threading.release()
                return

            index_name = data_json["indexNameCn"]
            p_day = data_json["endDate"]

            index_performance_dict["index_code"] = index_code
            index_performance_dict["index_name"] = index_name
            index_performance_dict["p_day"] = p_day

            # 获取接口中指数各阶段收益率情况
            one_month_yield_rate = 0
            three_month_yield_rate = 0
            this_year_yield_rate = 0
            one_year_yield_rate = 0
            three_year_yield_rate = 0
            five_year_yield_rate = 0

            if (data_json["oneMonth"] != None):
                one_month_yield_rate = float(data_json["oneMonth"])
            index_performance_dict["one_month_yield_rate"] = one_month_yield_rate
            if (data_json["threeMonth"] != None):
                three_month_yield_rate = float(data_json["threeMonth"])
            index_performance_dict["three_month_yield_rate"] = three_month_yield_rate
            if (data_json["thisYear"] != None):
                this_year_yield_rate = float(data_json["thisYear"])
            index_performance_dict["this_year_yield_rate"] = this_year_yield_rate
            if (data_json["oneYear"] != None):
                one_year_yield_rate = float(data_json["oneYear"])
            index_performance_dict["one_year_yield_rate"] = one_year_yield_rate
            if (data_json["threeYear"] != None):
                three_year_yield_rate = float(data_json["threeYear"])
            index_performance_dict["three_year_yield_rate"] = three_year_yield_rate
            if (data_json["fiveYear"] != None):
                five_year_yield_rate = float(data_json["fiveYear"])
            index_performance_dict["five_year_yield_rate"] = five_year_yield_rate

            # 如果3年年化收益率 或 5年年化收益率 满足需求
            if (three_year_yield_rate > self.three_year_yield_rate_standard) or (five_year_yield_rate > self.five_year_yield_rate_standard):
                # 获取跟踪这个指数的基金
                relative_funds_list = self.get_satisfied_index_relative_funds(index_code_re)
                # 如果没有跟踪的指数基金，则没有跟进的意义，放弃
                if(len(relative_funds_list)==0):
                    #same_time_threading.release()
                    return
                # 如果有跟踪指数基金，才有收集跟进的意义
                else:
                    index_performance_dict["relative_funds"] = relative_funds_list

                    # 如果当前采集的是全收益指数情况
                    if index_performance_dict["type"] == "total_return_index":
                        # 则需要收集常规指数的情况
                        index_performance_dict = self.parse_index_yield_performance(index_code_pair[0], index_performance_dict, header, proxy)
                    return index_performance_dict

        except Exception as e:
            # 日志记录
            msg = "从中证官网接口" + interface_url + '  ' + "获取指数过去表现 " + str(e)+ " 即将重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.call_interface_to_get_single_index_past_performance(index_code)

    def call_interface_to_get_single_index_past_performance(self, index_code_pair):

        '''
        调用接口获取指数过去几年的表现
        :param index_code_pair: 指数代码pair(指数代码, 全收益指数代码), 如 ('000979', 'H00979'), ('930927', None)
        :param satisfied_index_list: 满足收集指标的指数
        :param threadLock: 线程锁
        param: same_time_threading, 线程数量的限制
        :return:
        '''

        # 随机选取，伪装，隐藏UA和IP
        pick_an_int = random.randint(0, self.IP_UA_num - 1)
        #header = {"user-agent": self.ua_dict_list[random.randint(0, self.IP_UA_num*5 - 1)]['ua'], 'Connection': 'keep-alive'}
        header = {"user-agent": self.ua_dict_list[random.randint(0, self.IP_UA_num * 5 - 1)]['ua'], 'Connection': 'close'}
        proxy = {"http": 'http://{}:{}@{}'.format(conf.proxyIPUsername, conf.proxyIPPassword,
                                                  self.ip_address_dict_list[pick_an_int]['ip_address']),
                 "https": 'https://{}:{}@{}'.format(conf.proxyIPUsername, conf.proxyIPPassword,
                                                    self.ip_address_dict_list[pick_an_int]['ip_address'])}

        return self.parse_and_check_whether_an_excellent_index(index_code_pair, header, proxy)

    def check_all_index_and_get_all_excellent_index(self):
        '''
        收集满足预设年化收益率的指数信息
        :return:
        # 如 [{'index_code': '930758', 'index_name': '凤凰50', 'p_day': '2022-01-14', 'three_year_yield_rate': 27.12, 'five_year_yield_rate': 16.52, 'relative_funds': [{'512190': '浙商汇金中证凤凰50ETF'}, {'007431': '浙商汇金中证凤凰50ETF联接'}]}, ,,，，，]
        '''

        # 获取所有指数代码列表, [ (指数代码, 全收益指数代码)]
        #     如 [('000979', 'H00979'), ('000984', 'H00984'), ('930927', None), ,,,]
        index_code_list = self.call_interface_to_get_all_index_code_name_from_cs_index()

        # 满足条件的指数
        satisfied_index_list = []

        # 所有指数的代码平均分为多个区块，每份为X个元素可迭代对象list
        generate_splitted_lists = self.split_list_into_lists_with_certain_elements(index_code_list, self.max_index_codes)

        # 启用线程锁
        threadLock = threading.Lock()
        # 限制线程的最大数量
        same_time_threading = threading.Semaphore(self.max_thread_num)

        for splitted_list in generate_splitted_lists:
            # 获取到的IP和UA样式
            # 如 ([{'ip_address': '27.158.237.107:24135'}, {'ip_address': '27.151.158.219:50269'}], [{'ua': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1464.0 Safari/537.36'}, {'ua': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:17.0) Gecko/20100101 Firefox/17.0.6'}])
            self.ip_address_dict_list, self.ua_dict_list = disguise.Disguise().get_multi_IP_UA(self.IP_UA_num)
            # (指数代码, 全收益指数代码)
            for index_code_pair in splitted_list:
                # 启动线程
                index_performance_dict = self.call_interface_to_get_single_index_past_performance(index_code_pair)
                if index_performance_dict != None:
                    satisfied_index_list.append(index_performance_dict)

        return satisfied_index_list

    def parse_interface_to_get_index_relative_funds(self, index_code, header, proxy):
        '''
        从中证接口获取指数的相关基金代码和名称
        :param index_code: 指数代码（6位数字， 如 399997）
        :param header: 伪装的UA
        :param proxy: 伪装的IP
        :return: 指数相关基金的信息
        如 [{'512190': '浙商汇金中证凤凰50ETF'}, {'007431': '浙商汇金中证凤凰50ETF联接'}]
        '''
        # 地址模板
        interface_url = "https://www.csindex.com.cn/csindex-home/index-list/queryByIndexCode/"+index_code+"?indexCode="+index_code
        # 相关基金的列表
        relative_funds_list = []

        # 递归算法，处理异常
        try:
            # 增加连接重试次数,默认10次
            requests.adapters.DEFAULT_RETRIES = 10
            # 关闭多余的连接：requests使用了urllib3库，默认的http connection是close的，
            # requests设置False关闭
            s = requests.session()
            s.keep_alive = False

            # 忽略警告
            requests.packages.urllib3.disable_warnings()
            # 得到页面的信息
            raw_page = requests.get(interface_url, headers=header, proxies=proxy, verify=False, stream=False,
                                    timeout=self.timeout_limit ).text
            # 转换成字典数据
            raw_data = json.loads(raw_page)

            # 判断接口是否调用成功
            if (not raw_data["success"]):
                return []

            # 获取data数据
            data_json = raw_data["data"]
            if (data_json == None):
                return []

            # 遍历获取到的接口数据
            for fund_info in data_json:
                fund_dict = dict()
                fund_code = fund_info["productCode"]
                fund_name = fund_info["fundName"]
                fund_dict[fund_code] = fund_name
                relative_funds_list.append(fund_dict)

            # 返回 如， [{'512190': '浙商汇金中证凤凰50ETF'}, {'007431': '浙商汇金中证凤凰50ETF联接'}]
            return relative_funds_list

        except Exception as e:
            # 日志记录
            msg = "从中证官网接口 " + interface_url + '  ' + "获取相关基金产品失败 " + str(e) + " 即将重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_satisfied_index_relative_funds(index_code)


    def get_satisfied_index_relative_funds(self, index_code):
        '''
        获取满足指标的指数的相关基金
        :param index_code: 指数代码（6位数字， 如 399997）
        :return:
        '''

        # 随机选取，伪装，隐藏UA和IP
        pick_an_int = random.randint(0, self.IP_UA_num - 1)
        #header = {"user-agent": self.ua_dict_list[random.randint(0, self.IP_UA_num*5 - 1)]['ua'], 'Connection': 'keep-alive'}
        header = {"user-agent": self.ua_dict_list[random.randint(0, self.IP_UA_num * 5 - 1)]['ua'], 'Connection': 'close'}
        proxy = {"http": 'http://{}:{}@{}'.format(conf.proxyIPUsername, conf.proxyIPPassword,
                                                  self.ip_address_dict_list[pick_an_int]['ip_address']),
                 "https": 'https://{}:{}@{}'.format(conf.proxyIPUsername, conf.proxyIPPassword,
                                                    self.ip_address_dict_list[pick_an_int]['ip_address'])}

        return self.parse_interface_to_get_index_relative_funds(index_code, header, proxy)


    def collect_and_save(self):
        # 存储符合收集标准的指数
        # 如 [{'index_code': '930758', 'index_name': '凤凰50', 'p_day': '2022-01-14', 'three_year_yield_rate': 27.12, 'five_year_yield_rate': 16.52, 'relative_funds': [{'512190': '浙商汇金中证凤凰50ETF'}, {'007431': '浙商汇金中证凤凰50ETF联接'}]}, ,,，，，]
        satisfied_index_list = self.check_all_index_and_get_all_excellent_index()

        # 遍历
        for index_info in satisfied_index_list:

            # 常规指数的收益率信息
            # 指数代码
            index_code_re = ""
            # 指数名称
            index_name_re = ""
            # 指数近1月年化收益率
            one_month_yield_rate_re = 0
            # 指数近3月年化收益率
            three_month_yield_rate_re = 0
            # 指数年至今年化收益率
            this_year_yield_rate_re = 0
            # 指数近1年年化收益率
            one_year_yield_rate_re = 0
            # 指数近3年年化收益率
            three_year_yield_rate_re = 0
            # 指数近5年年化收益率
            five_year_yield_rate_re = 0

            # 全收益指数收益率信息
            # 指数代码
            index_code_tr = ""
            # 指数名称
            index_name_tr = ""
            # 指数近1月年化收益率
            one_month_yield_rate_tr = 0
            # 指数近3月年化收益率
            three_month_yield_rate_tr = 0
            # 指数年至今年化收益率
            this_year_yield_rate_tr = 0
            # 指数近1年年化收益率
            one_year_yield_rate_tr = 0
            # 指数近3年年化收益率
            three_year_yield_rate_tr = 0
            # 指数近5年年化收益率
            five_year_yield_rate_tr = 0


            # 如果该指数存在全收益指数
            if index_info.get("type") == "total_return_index":
                # 常规指数的收益率信息
                # 指数代码
                index_code_re = index_info.get("index_code_re")
                # 指数名称
                index_name_re = index_info.get("index_name_re")
                # 指数近1月年化收益率
                one_month_yield_rate_re = index_info.get("one_month_yield_rate_re")
                # 指数近3月年化收益率
                three_month_yield_rate_re = index_info.get("three_month_yield_rate_re")
                # 指数年至今年化收益率
                this_year_yield_rate_re = index_info.get("this_year_yield_rate_re")
                # 指数近1年年化收益率
                one_year_yield_rate_re = index_info.get("one_year_yield_rate_re")
                # 指数近3年年化收益率
                three_year_yield_rate_re = index_info.get("three_year_yield_rate_re")
                # 指数近5年年化收益率
                five_year_yield_rate_re = index_info.get("five_year_yield_rate_re")

                # 全收益指数收益率信息
                # 指数代码
                index_code_tr = index_info.get("index_code")
                # 指数名称
                index_name_tr = index_info.get("index_name")
                # 指数近1月年化收益率
                one_month_yield_rate_tr = index_info.get("one_month_yield_rate")
                # 指数近3月年化收益率
                three_month_yield_rate_tr = index_info.get("three_month_yield_rate")
                # 指数年至今年化收益率
                this_year_yield_rate_tr = index_info.get("this_year_yield_rate")
                # 指数近1年年化收益率
                one_year_yield_rate_tr = index_info.get("one_year_yield_rate")
                # 指数近3年年化收益率
                three_year_yield_rate_tr = index_info.get("three_year_yield_rate")
                # 指数近5年年化收益率
                five_year_yield_rate_tr = index_info.get("five_year_yield_rate")

            elif index_info.get("type") == "regular_index":
                # 常规指数收益率信息
                # 指数代码
                index_code_re = index_info.get("index_code")
                # 指数名称
                index_name_re = index_info.get("index_name")
                # 指数近1月年化收益率
                one_month_yield_rate_re = index_info.get("one_month_yield_rate")
                # 指数近3月年化收益率
                three_month_yield_rate_re = index_info.get("three_month_yield_rate")
                # 指数年至今年化收益率
                this_year_yield_rate_re = index_info.get("this_year_yield_rate")
                # 指数近1年年化收益率
                one_year_yield_rate_re = index_info.get("one_year_yield_rate")
                # 指数近3年年化收益率
                three_year_yield_rate_re = index_info.get("three_year_yield_rate")
                # 指数近5年年化收益率
                five_year_yield_rate_re = index_info.get("five_year_yield_rate")



            # 业务日期
            p_day =  index_info.get("p_day")
            # 所有关联的跟踪指数基金
            relative_funds = index_info.get("relative_funds")
            # 遍历这些指数基金
            for fund in relative_funds:
                for relative_fund_code in fund:
                    # 基金名称
                    relative_fund_name = fund.get(relative_fund_code)
                    try:
                        # 如果该指数存在全收益指数, 则将常规指数信息和全收益指数信息全存进数据库
                        if index_info.get("type") == "total_return_index":
                            # 将优秀的指数信息及其相关基金产品存入数据库
                            index_excellent_performance_indices_di_mapper.IndexExcellentPerformanceIndicesDiMapper().insert_regular_and_total_return_excellent_indexes(
                                index_code_re, index_name_re, '中证', one_month_yield_rate_re,
                                three_month_yield_rate_re,
                                this_year_yield_rate_re, one_year_yield_rate_re, three_year_yield_rate_re,
                                five_year_yield_rate_re, index_code_tr, index_name_tr, one_month_yield_rate_tr,
                                three_month_yield_rate_tr,
                                this_year_yield_rate_tr, one_year_yield_rate_tr, three_year_yield_rate_tr,
                                five_year_yield_rate_tr, relative_fund_code, relative_fund_name, p_day)
                            # 日志记录
                            msg = '将从中证官网接口获取的优异指数 ' + p_day + " " + index_code_re + " " + index_name_re + ' 存入数据库时成功'
                            custom_logger.CustomLogger().log_writter(msg, 'info')

                        # 如果该指数仅存在常规指数, 则将常规指数存进数据库
                        elif index_info.get("type") == "regular_index":
                            # 将优秀的指数信息及其相关基金产品存入数据库
                            index_excellent_performance_indices_di_mapper.IndexExcellentPerformanceIndicesDiMapper().insert_regular_excellent_indexes(
                                index_code_re, index_name_re, '中证', one_month_yield_rate_re, three_month_yield_rate_re,
                                this_year_yield_rate_re, one_year_yield_rate_re, three_year_yield_rate_re, five_year_yield_rate_re,
                                relative_fund_code, relative_fund_name, p_day)
                            # 日志记录
                            msg = '将从中证官网接口获取的优异指数 ' + p_day + " " + index_code_re + " " + index_name_re + ' 存入数据库时成功'
                            custom_logger.CustomLogger().log_writter(msg, 'info')

                    except Exception as e:
                        # 日志记录
                        msg = '将从中证官网接口获取的优异指数 ' + p_day + " " + index_code_re + " " + index_name_re + " " + ' 存入数据库时错误  ' + str(
                            e)
                        custom_logger.CustomLogger().log_writter(msg, 'error')



    """
    将列表平均分，平均分后，每份个数为num
    :param index_code_list: 获取指数代码列表, [ (指数代码, 全收益指数代码)], 如 [('000979', 'H00979'), ('000984', 'H00984'), ('930927', None), ,,,]
    :param num: 平分后每份列表的的个数num
    :return
    如，
    [['000300', '000905', '000852', '000903', '000016', '000688', '000001', '000009', '000010', '000015', '000018', '000021', '000029', '000036', '000037', '000038', '000042', '000043', '000044', '000046'], 
    ['000048', '000056', '000063', '000064', '000065', '000066', '000068', '000069', '000802', '000814', '000819', '000827', '000901', '000906', '000913', '000914', '000928', '000932', '000933', '000934'], ,,,]
    """
    def split_list_into_lists_with_certain_elements(self, index_code_list, num):
        splitted_lists = list()
        for i in range(0, len(index_code_list), num):
            splitted_lists.append(index_code_list[i:i + num])

        return splitted_lists

    """
    获取特定数据源的指数及其相关全收益指数
    :return , 例如 {'000050': {'index_code': '000050', 'index_name': '上证50等权重指数', 'index_code_tr': 'H00050', 'index_name_tr': '上证50等权重全收益指数'}, 
    '000052': {'index_code': '000052', 'index_name': '上证50基本面加权指数', 'index_code_tr': 'H00052', 'index_name_tr': '上证50基本面加权全收益指数'},,,,]
    """
    def get_index_and_total_return_index_dict(self):
        # 指数及其对应的全收益指数信息
        index_relate_tr_dict = dict()
        # 指数及其相关全收益指数
        index_list = fin_data_total_return_indexes_list_mapper.FinDataTotalReturnIndexesListMapper().select_index_name_feature()
        # 将指数及其相关全收益指数转为dict
        for unit in index_list:
            index_code = unit.get("index_code")
            index_relate_tr_dict[index_code] = unit

        return index_relate_tr_dict

    def main(self):
        # 指数及其对应的全收益指数信息
        self.index_relate_tr_dict = self.get_index_and_total_return_index_dict()
        self.collect_and_save()

if __name__ == '__main__':
    time_start = time.time()
    go = CollectExcellentIndexFromCSIndexSingleThread()
    go.main()
    #result = go.get_index_and_total_return_index_dict()
    #result = go.call_interface_to_get_all_index_code_name_from_cs_index()
    #result = go.call_interface_to_get_single_index_past_performance("399997")
    #result = go.check_all_index_and_get_all_excellent_index()
    #result = go.get_satisfied_index_relative_funds("930758")
    #go.call_interface_to_get_single_index_past_performance("H50059")
    #print(result)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))