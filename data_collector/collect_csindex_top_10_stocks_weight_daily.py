#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import json
import sys
import threading
import time

import requests

sys.path.append("..")
import parsers.disguise as disguise
import log.custom_logger as custom_logger
import db_mapper.target_pool.investment_target_mapper as investment_target_mapper
import db_mapper.financial_data.index_constituent_stocks_weight_mapper as index_constituent_stocks_weight_mapper


class CollectCSIndexTop10StocksWeightDaily:
    # 从中证指数官网获取指数的前十权重数据，并存入数据库
    # 运行频率：每天

    def __init__(self):
        pass

    def parse_page_content(self, index_id, header, proxy):
        # 解析中证官网页信息
        # index_id: 指数代码（6位数字， 如 399997）
        # page_address，地址
        # header，伪装的UA
        # proxy，伪装的IP
        # 返回 tuple (截止日期, [前十成份股代码，名称，权重])
        # 如 ( '2021-09-24', [['600809', '山西汾酒', 'sh','16.766190846153634'], ['600519', '贵州茅台', 'sh','13.277568906087126'], ,,,,])

        # 地址模板
        page_address = 'https://www.csindex.com.cn/csindex-home/index/weight/top10/' + index_id

        # 返回的前十成份股信息，成份股代码，名称，权重
        # [['600809', '山西汾酒', '16.766190846153634'], ['600519', '贵州茅台', '13.277568906087126'], ,,,,]
        top_10_stocks_detail_info_list = []

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
            raw_page = requests.get(page_address, headers=header, proxies=proxy, verify=False, stream=False,
                                    timeout=3).text
            # 转换成字典数据
            data_json = json.loads(raw_page)
            # 获取更新日期
            p_day = data_json['data']['updateDate']
            # 遍历成分股权重列表
            for stock_info in data_json['data']['weightList']:
                # 单个成份股的关键信息，如 ['600519', '贵州茅台', 'sh','13.277568906087126']
                stock_detail_info_list = []
                stock_detail_info_list.append(stock_info['securityCode'])
                stock_detail_info_list.append(stock_info['securityName'])
                if 'Shenzhen' in stock_info['marketNameEn']:
                    stock_detail_info_list.append('sz')
                elif 'Shanghai' in stock_info['marketNameEn']:
                    stock_detail_info_list.append('sh')
                else:
                    stock_detail_info_list.append('unknown')
                stock_detail_info_list.append(stock_info['preciseWeight'])
                top_10_stocks_detail_info_list.append(stock_detail_info_list)
            # 按成分股股票代码从大到小排序
            top_10_stocks_detail_info_list.sort(key=lambda x: x[0], reverse=True)
            # 返回如 ( '2021-09-24', [['600809', '山西汾酒', 'sh','16.766190846153634'], ['600519', '贵州茅台', 'sh','13.277568906087126'], ,,,,])
            return p_day, top_10_stocks_detail_info_list

            # 日志记录
            # msg = "从中证官网 " + page_address + '  ' + "获取了 " + expiration_date + "的前十成份股信息"
            # custom_logger.CustomLogger().log_writter(msg, lev='info')

        # 如果读取超时，重新在执行一遍解析页面
        except requests.exceptions.ReadTimeout:
            # 日志记录
            msg = "从中证官网" + page_address + '  ' + "获取前十成份股信息 " + " ReadTimeout。" + " 即将重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_index_latest_constituent_stock_and_weight(index_id)

        # 如果连接请求超时，重新在执行一遍解析页面
        except requests.exceptions.ConnectTimeout:
            # 日志记录
            msg = "从中证官网" + page_address + '  ' + "获取前十成份股信息 " + " ConnectTimeout。" + " 即将重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_index_latest_constituent_stock_and_weight(index_id)

        # 如果请求超时，重新在执行一遍解析页面
        except requests.exceptions.Timeout:
            # 日志记录
            msg = "从中证官网" + page_address + '  ' + "获取前十成份股信息 " + " Timeout。" + " 即将重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_index_latest_constituent_stock_and_weight(index_id)

        except Exception as e:
            # 日志记录
            msg = "从中证官网" + page_address + '  ' + "获取前十成份股信息时 " + str(e)
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_index_latest_constituent_stock_and_weight(index_id)

    def get_cs_index_from_index_target(self):
        # 从标的池中获取中证公司的指数
        # 返回：指数代码及对应的指数名称的字典
        # {'399997': '中证白酒指数', '000932': '中证主要消费', '399965': '中证800地产', '399986': '中证银行指数', '000036': '上证主要消费行业指数'}

        # 存放指数代码及对应的指数名称的字典
        target_cs_index_dict = dict()

        # [{'index_code': '399965', 'index_name': '中证800地产', 'index_code_with_init': 'sz399965',
        # 'index_code_with_market_code': '399965.XSHE'},，，]
        target_cs_index_info_list = investment_target_mapper.InvestmentTargetMapper().get_given_index_company_index("index", "active", "buy", "中证")
        for info in target_cs_index_info_list:
            target_cs_index_dict[info["index_code"]] = info["index_name"]
        return target_cs_index_dict

    def get_single_index_latest_constituent_stock_and_weight(self, index_id):
        # 从中证官网获取单个指数最新的前十成份股和权重信息
        # index_id: 指数代码（6位数字， 如 399997）
        # 返回 tuple (截止日期, [前十成份股代码，名称，权重])
        # 如 ( '2021-09-24', [['600809', '山西汾酒', 'sh','16.766190846153634'], ['600519', '贵州茅台', 'sh','13.277568906087126'],,,,,])

        # 伪装，隐藏UA和IP
        header, proxy = disguise.Disguise().assemble_header_proxy()
        return self.parse_page_content(index_id, header, proxy)

    def check_if_saved_before(self, index_code, p_day, stocks_detail_info_list):
        # 与数据库的内容对比，是否已存过
        # index_code,指数代码，399396
        # p_day, 更新日期，2021-12-13
        # stocks_detail_info_list, 成分股的信息， [['600809', '山西汾酒', 'sh','16.766190846153634'], ['600519', '贵州茅台', 'sh','13.277568906087126'],,,,,])
        # 返回：如果存储过，则返回True; 未储存过，则返回False

        db_index_content = index_constituent_stocks_weight_mapper.IndexConstituentStocksWeightMapper().get_db_index_company_index_latest_component_stocks('中证官网', index_code)
        # 成分股个数
        file_content_len = len(stocks_detail_info_list)
        # 数据库中的指数成分股个数
        db_index_content_len = len(db_index_content)
        # 对比文件内容中的成分股 与 数据库中的指数成分股 个数是否一致
        if (file_content_len != db_index_content_len):
            return False

        for i in range(file_content_len):
            # 对比股票代码是否一致
            if (stocks_detail_info_list[i][0] != db_index_content[i]["stock_code"]):
                return False
            # 对比股票权重是否一致
            elif (stocks_detail_info_list[i][3] != float(db_index_content[i]["weight"])):
                return False
            # 对比发布日期是否一致
            elif (p_day != str(db_index_content[i]["p_day"])):
                return False
        return True

    def save_index_info_into_db(self, index_code, index_name, p_day, top_10_stocks_detail_info_list):
        # 将指数成分股信息存入数据库
        # index_code，指数代码，399396
        # index_name，指数名称，国证食品饮料行业
        # p_day，更新日期，2021-12-13
        # top_10_stocks_detail_info_list，成分股的信息， [['600809', '山西汾酒', 'sh','16.766190846153634'], ['600519', '贵州茅台', 'sh','13.277568906087126']
        # 返回：存入数据库

        for stock_info in top_10_stocks_detail_info_list:
            # 股票代码，如 600887
            stock_code = stock_info[0]
            # 股票名称，如 伊利股份
            stock_name = stock_info[1].replace(' ', '')
            # 股票交易所地点，如 sh，sz
            stock_exchange_location = stock_info[2]
            # 权重，如 16.766190846153634
            weight = stock_info[3]
            # 交易所全球代码
            if stock_exchange_location == 'sh':
                stock_market_code = 'XSHG'
            elif stock_exchange_location == 'sz':
                stock_market_code = 'XSHE'
            else:
                stock_market_code = 'UNKNOWN'

            try:
                # 将指数的信息存入数据库
                index_constituent_stocks_weight_mapper.IndexConstituentStocksWeightMapper().save_index_info(index_code, index_name, stock_code, stock_name, stock_exchange_location,
                                    stock_market_code, weight, '中证官网', '中证', p_day)
            except Exception as e:
                # 日志记录
                msg = '将从中证官网获取的' + p_day + index_code + index_name + '的前十大权重股存入数据库时错误  ' + str(e)
                custom_logger.CustomLogger().log_writter(msg, 'error')

    def collect_target_index_stock_info_by_single_thread(self):
        # 单线程收集目标池中的中证指数每日前十大权重股构成,并存入数据库

        # 获取标的池中关于 中证公司的 指数代码及指数名称
        # 如 {'399997': '中证白酒指数', '000932': '中证主要消费', '399965': '中证800地产', '399986': '中证银行指数', '000036': '上证主要消费行业指数'}
        cs_target_indexes_names_dict = self.get_cs_index_from_index_target()
        # 遍历这些指数
        for index_code in cs_target_indexes_names_dict:
            self.get_check_and_save_index_info(index_code, cs_target_indexes_names_dict)

    def get_check_and_save_index_info(self, index_code, target_cs_index_dict, threadLock):
        # 从接口获取前十成分股信息，检查是否存储过，并存储指数成分股及权重信息
        # index_code,指数代码，399997
        # target_cs_index_dict，指数代码及对应的指数名称的字典
        # threadLock：线程锁
        # 如 {'399997': '中证白酒指数', '000932': '中证主要消费', '399965': '中证800地产', '399986': '中证银行指数', '000036': '上证主要消费行业指数'}

        # 获取 该指数的最新更新日期及前十成份股信息
        p_day, top_10_stocks_detail_info_list = self.get_single_index_latest_constituent_stock_and_weight(index_code)
        # 指数名称，如 中证白酒
        index_name = target_cs_index_dict[index_code]

        is_saved_before = self.check_if_saved_before(index_code, p_day, top_10_stocks_detail_info_list)
        # 如果储存过，则跳过
        if (is_saved_before):
            # 获取锁，用于线程同步
            threadLock.acquire()
            # 日志记录
            msg = p_day + " " + index_code + " " + index_name + " 的前十权重股曾经储存过，无需再存储"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 释放锁，开启下一个线程
            threadLock.release()
        # 如果未存储过，则存入数据库
        else:
            self.save_index_info_into_db(index_code, index_name, p_day, top_10_stocks_detail_info_list)
            # 获取锁，用于线程同步
            threadLock.acquire()
            # 日志记录
            msg = p_day + " " + index_code + " " + index_name + " 的前十权重股未储存过，已更新指数信息"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 释放锁，开启下一个线程
            threadLock.release()

    def collect_target_index_stock_info_by_multi_threads(self):
        # 多线程收集目标池中的中证指数每日前十大权重股构成,并存入数据库

        # 获取标的池中关于 中证公司的 指数代码及指数名称
        # 如 {'399997': '中证白酒指数', '000932': '中证主要消费', '399965': '中证800地产', '399986': '中证银行指数', '000036': '上证主要消费行业指数'}
        cs_target_indexes_names_dict = self.get_cs_index_from_index_target()

        # 启用多线程
        running_threads = []
        # 启用线程锁
        threadLock = threading.Lock()

        # 遍历这些指数
        for index_code in cs_target_indexes_names_dict:
            # 启动线程
            running_thread = threading.Thread(target=self.get_check_and_save_index_info,
                                              kwargs={"index_code": index_code,
                                                      "target_cs_index_dict": cs_target_indexes_names_dict,
                                                      "threadLock": threadLock})
            running_threads.append(running_thread)

            # 开启新线程
        for mem in running_threads:
            mem.start()

            # 等待所有线程完成
        for mem in running_threads:
            mem.join()

    def main(self):
        # self.collect_target_index_stock_info_by_single_thread()
        self.collect_target_index_stock_info_by_multi_threads()


if __name__ == '__main__':
    time_start = time.time()
    go = CollectCSIndexTop10StocksWeightDaily()
    # go.get_single_index_latest_constituent_stock_and_weight('399997')
    # real_time_pe_ttm = go.get_single_index_latest_constituent_stock_and_weight('399997')
    # print(real_time_pe_ttm)
    #result = go.get_cs_index_from_index_target()
    #print(result)
    go.main()
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))
