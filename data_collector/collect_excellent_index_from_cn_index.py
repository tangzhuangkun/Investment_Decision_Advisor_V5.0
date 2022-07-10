#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import time
import requests
import json
import threading
import random

import sys
sys.path.append("..")
import parsers.disguise as disguise
import log.custom_logger as custom_logger
import database.db_operator as db_operator
import conf

class CollectExcellentIndexFromCNIndex:
    # 从国证指数官网接口收集过去几年表现优异的指数

    def __init__(self):

        # 衡量标准
        # 3年年化收益率
        self.three_year_yield_rate_standard = 20
        # 5年年化收益率
        self.five_year_yield_rate_standard = 15
        # 最大线程数
        self.max_thread_num = 10
        # 同时获取x个IP和5xUA
        self.IP_UA_num = 5
        # 将中证所有指数代码分成多个区块，每个区块最多拥有多少个指数代码
        self.max_index_codes = 50
        # 每个区块执行的时间
        self.sleep_time = 30

    def parse_interface_to_get_index_code_content(self, header, proxy):
        '''
        从国证接口获取全部指数的代码和名称
        :param header: 伪装的UA
        :param proxy: 伪装的IP
        :return: 指数代码的set
        如 {'000951', '000131', '000938', '930902', '931036', ，，，}
        '''

        # 指数代码
        index_code_list = list()

        # 接口地址
        interface_url = "http://www.cnindex.com.cn/index/indexList?channelCode=-1&rows=20000&pageNum=1"

        # 递归算法，处理异常
        try:
            # 增加连接重试次数,默认10次
            requests.adapters.DEFAULT_RETRIES = 10
            # 关闭多余的连接：requests使用了urllib3库，默认的http connection是keep-alive的，
            # requests设置False关闭
            s = requests.session()
            s.keep_alive = False

            # 忽略警告
            requests.packages.urllib3.disable_warnings()
            # 得到页面的信息
            raw_page = requests.get(interface_url, headers=header, proxies=proxy, verify=False, stream=False,
                                    timeout=10).text

            # 转换成字典数据
            data_json = json.loads(raw_page)["data"]["rows"]

            for index_info in data_json:
                index_code_list.append(index_info["indexcode"])

            return index_code_list


        except Exception as e:
            # 日志记录
            msg = "从国证官网接口 " + interface_url + '  ' + "获取全部指数的代码失败 " + str(e) + " 即将重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.call_interface_to_get_all_index_code_from_cn_index()

    def call_interface_to_get_all_index_code_from_cn_index(self):
        '''
        调用接口，从国证指数官网获取全部指数代码和名称
        return: 数代码的set
        '''

        # 此处获取全部指数代码和名称可能存在 超时，链接出错等原因导致 获取到信息的时间过长，约20秒-40秒
        # 代理IP存活时长一般为1分钟左右，若一口气调用太多代理IP，大概率存在代理IP已失活
        # 此处为该脚本第一次调用 代理IP的API，且存在获取时间过长，因此仅调用1个IP和1个UA
        # 伪装，隐藏UA和IP
        header, proxy = disguise.Disguise().assemble_header_proxy()

        return self.parse_interface_to_get_index_code_content(header, proxy)


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
        interface_url = "http://www.cnindex.com.cn/info/fund?indexCode="+index_code+"&pageNum=1&rows=200"
        # 相关基金的列表
        relative_funds_list = []

        # 递归算法，处理异常
        try:
            # 增加连接重试次数,默认10次
            requests.adapters.DEFAULT_RETRIES = 10
            # 关闭多余的连接：requests使用了urllib3库，默认的http connection是keep-alive的，
            # requests设置False关闭
            s = requests.session()
            s.keep_alive = False

            # 忽略警告
            requests.packages.urllib3.disable_warnings()
            # 得到页面的信息
            raw_page = requests.get(interface_url, headers=header, proxies=proxy, verify=False, stream=False,
                                    timeout=10).text
            # 转换成字典数据
            raw_data = json.loads(raw_page)

            # 判断接口是否调用成功
            if (raw_data["code"] != 200):
                return []

            # 如果目前无跟踪产品
            if (raw_data["data"]==None):
                return []

            # 获取data数据
            data_json = raw_data["data"]["rows"]
            # 遍历获取到的接口数据
            for fund_info in data_json:
                fund_dict = dict()
                fund_code = fund_info["fundCode"]
                fund_name = fund_info["fundName"]
                fund_dict[fund_code] = fund_name
                relative_funds_list.append(fund_dict)

            # 返回 如， [{'512190': '浙商汇金中证凤凰50ETF'}, {'007431': '浙商汇金中证凤凰50ETF联接'}]
            return relative_funds_list

        except Exception as e:
            # 日志记录
            msg = "从国证官网接口 " + interface_url + '  ' + "获取相关基金产品失败 " + str(e) + " 即将重试"
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
        header = {"user-agent": self.ua_dict_list[random.randint(0, self.IP_UA_num*5- 1)]['ua'], 'Connection': 'close'}
        proxy = {"http": 'http://{}:{}@{}'.format(conf.proxyIPUsername, conf.proxyIPPassword,
                                                  self.ip_address_dict_list[pick_an_int]['ip_address']),
                 "https": 'https://{}:{}@{}'.format(conf.proxyIPUsername, conf.proxyIPPassword,
                                                    self.ip_address_dict_list[pick_an_int]['ip_address'])}

        return self.parse_interface_to_get_index_relative_funds(index_code, header, proxy)

    def parse_and_check_whether_an_excellent_index(self,index_code, satisfied_index_list, threadLock, same_time_threading, header, proxy):
        '''
        解析判断是否为
        解析接口内容，获取单个指数过去几年的表现
        :param index_code: 指数代码（6位数字， 如 399997）
        :param satisfied_index_list: 满足收集指标的指数
        :param threadLock: 线程锁
        :param: same_time_threading, 线程数量的限制
        :param header: 伪装的UA
        :param proxy: 伪装的IP
        :return:
        '''

        interface_url = "http://www.cnindex.com.cn/index-income?indexcode="+index_code

        # 指数近3，5年表现的字典
        index_performance_dict = dict()

        # 递归算法，处理异常
        try:
            # 增加连接重试次数,默认10次
            requests.adapters.DEFAULT_RETRIES = 10
            # 关闭多余的连接：requests使用了urllib3库，默认的http connection是keep-alive的，
            # requests设置False关闭
            s = requests.session()
            s.keep_alive = False

            # 忽略警告
            requests.packages.urllib3.disable_warnings()
            # 得到页面的信息
            raw_page = requests.get(interface_url, headers=header, proxies=proxy, verify=False, stream=False,
                                    timeout=10).text

            # 转换成字典数据
            raw_data = json.loads(raw_page)
            # 判断接口是否调用成功
            if (raw_data["code"] != 200):
                same_time_threading.release()
                return

            # 获取data数据
            data_json = raw_data["data"]
            if(data_json==None):
                same_time_threading.release()
                return

            index_name = data_json["indexname"]
            p_day = data_json["calDate"]

            index_performance_dict["index_code"] = index_code
            index_performance_dict["index_name"] = index_name
            index_performance_dict["p_day"] = p_day

            three_year_yield_rate = 0
            five_year_yield_rate = 0

            if (data_json["annIncome3y"] != "-"):
                three_year_yield_rate = float(data_json["annIncome3y"][:-1])
                index_performance_dict["three_year_yield_rate"] = three_year_yield_rate
            if (data_json["annIncome5y"] != "-"):
                five_year_yield_rate = float(data_json["annIncome5y"][:-1])
                index_performance_dict["five_year_yield_rate"] = five_year_yield_rate

            # 如果3年年化收益率 和 5年年化收益率 满足需求
            if (three_year_yield_rate > self.three_year_yield_rate_standard and five_year_yield_rate > self.five_year_yield_rate_standard):
                # 获取跟踪这个指数的基金
                relative_funds_list = self.get_satisfied_index_relative_funds(index_code)
                # 如果没有跟踪的指数基金，则没有跟进的意义，放弃获取相关基金产品失败
                if(len(relative_funds_list)==0):
                    same_time_threading.release()
                    return
                # 如果有跟踪指数基金，才有收集跟进的意义
                else:
                    index_performance_dict["relative_funds"] = relative_funds_list

                # 获取锁，用于线程同步
                threadLock.acquire()
                satisfied_index_list.append(index_performance_dict)
                # 释放锁，开启下一个线程
                threadLock.release()

            same_time_threading.release()

        except Exception as e:
            # 日志记录
            msg = "从国证官网接口 " + interface_url + '  ' + "获取指数过去表现 " + str(e)+ " 即将重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.call_interface_to_get_single_index_past_performance(index_code, satisfied_index_list, threadLock, same_time_threading)

    def call_interface_to_get_single_index_past_performance(self, index_code, satisfied_index_list, threadLock, same_time_threading):

        '''
        调用接口获取指数过去几年的表现
        :param index_code: 指数代码（6位数字， 如 399997）
        :param satisfied_index_list: 满足收集指标的指数
        :param threadLock: 线程锁
        param: same_time_threading, 线程数量的限制
        :return:
        '''

        # 随机选取，伪装，隐藏UA和IP
        pick_an_int = random.randint(0, self.IP_UA_num - 1)
        header = {"user-agent": self.ua_dict_list[random.randint(0, self.IP_UA_num*5- 1)]['ua'], 'Connection': 'close'}
        proxy = {"http": 'http://{}:{}@{}'.format(conf.proxyIPUsername, conf.proxyIPPassword,
                                                  self.ip_address_dict_list[pick_an_int]['ip_address']),
                 "https": 'https://{}:{}@{}'.format(conf.proxyIPUsername, conf.proxyIPPassword,
                                                    self.ip_address_dict_list[pick_an_int]['ip_address'])}


        return self.parse_and_check_whether_an_excellent_index(index_code, satisfied_index_list, threadLock, same_time_threading, header, proxy)

    def check_all_index_and_get_all_excellent_index(self):
        '''
        收集满足预设年化收益率的指数信息
        :return:
        # 如 [{'index_code': '930758', 'index_name': '凤凰50', 'p_day': '2022-01-14', 'three_year_yield_rate': 27.12, 'five_year_yield_rate': 16.52, 'relative_funds': [{'512190': '浙商汇金中证凤凰50ETF'}, {'007431': '浙商汇金中证凤凰50ETF联接'}]}, ,,，，，]
        '''

        # 获取所有指数的代码
        index_code_list = self.call_interface_to_get_all_index_code_from_cn_index()
        # 满足条件的指数
        satisfied_index_list = []
        # 所有指数的代码平均分为多个区块，每份为x个元素可迭代对象list
        generate_splitted_lists = self.split_list_into_lists_with_certain_elements(index_code_list,
                                                                                   self.max_index_codes)

        # 启用线程锁
        threadLock = threading.Lock()
        # 限制线程的最大数量
        same_time_threading = threading.Semaphore(self.max_thread_num)

        for splitted_list in generate_splitted_lists:
            # 获取到的IP和UA样式
            # 如 ([{'ip_address': '27.158.237.107:24135'}, {'ip_address': '27.151.158.219:50269'}], [{'ua': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1464.0 Safari/537.36'}, {'ua': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:17.0) Gecko/20100101 Firefox/17.0.6'}])
            self.ip_address_dict_list, self.ua_dict_list = disguise.Disguise().get_multi_IP_UA(self.IP_UA_num)
            for index_code in splitted_list:
                same_time_threading.acquire()
                # 启动线程
                threading.Thread(target=self.call_interface_to_get_single_index_past_performance,
                                                  kwargs={"index_code": index_code,
                                                          "satisfied_index_list": satisfied_index_list,
                                                          "threadLock": threadLock,
                                                          "same_time_threading":same_time_threading
                                                          }).start()

            # 每个区块，50个指数，有x秒的时间执行，
            # x秒之后，如果还未成功获取数据的，可能代理IP已被网站屏蔽，未获取到数据的指数亦可放弃
            # x秒之后，启动下一个区块的数据抓取工作
            time.sleep(self.sleep_time)

        return satisfied_index_list

    def collect_and_save(self):
        # 存储符合收集标准的指数
        # 如 [{'index_code': '930758', 'index_name': '凤凰50', 'p_day': '2022-01-14', 'three_year_yield_rate': 27.12, 'five_year_yield_rate': 16.52, 'relative_funds': [{'512190': '浙商汇金中证凤凰50ETF'}, {'007431': '浙商汇金中证凤凰50ETF联接'}]}, ,,，，，]
        satisfied_index_list = self.check_all_index_and_get_all_excellent_index()

        # 遍历
        for index_info in satisfied_index_list:
            # 指数代码
            index_code = index_info.get("index_code")
            # 指数名称
            index_name = index_info.get("index_name")
            # 指数近3年年化收益率
            three_year_yield_rate = index_info.get("three_year_yield_rate")
            # 指数近5年年化收益率
            five_year_yield_rate = index_info.get("five_year_yield_rate")
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
                        # 插入的SQL
                        inserting_sql = "INSERT INTO index_excellent_performance_indices_di(index_code,index_name," \
                                        "index_company,three_year_yield_rate,five_year_yield_rate,relative_fund_code," \
                                        "relative_fund_name,p_day)" \
                                        "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                                            index_code,index_name, '国证', three_year_yield_rate,five_year_yield_rate,
                                            relative_fund_code,relative_fund_name,p_day)
                        db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)
                        # 日志记录
                        msg = '将从国证官网接口获取的优异指数' + p_day + index_code + index_name + ' 存入数据库成功'
                        custom_logger.CustomLogger().log_writter(msg, 'info')

                    except Exception as e:
                        # 日志记录
                        msg = '将从国证官网接口获取的优异指数' + p_day + index_code + index_name + ' 存入数据库时错误  ' + str(e)
                        custom_logger.CustomLogger().log_writter(msg, 'error')

    def split_list_into_lists_with_certain_elements(self, lt, n):
        '''
        将lt平均分，平均分后，每份个数为n
        :param lt: 为一个列表
        :param n: 平分后每份列表的的个数n
        :return:
        '''
        for i in range(0, len(lt), n):
            yield lt[i:i + n]

    def main(self):
        self.collect_and_save()



if __name__ == '__main__':
    time_start = time.time()
    go = CollectExcellentIndexFromCNIndex()
    go.main()
    #result = go.get_satisfied_index_relative_funds("483060")
    #result = go.call_interface_to_get_all_index_code_from_cn_index()
    #print(result)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))