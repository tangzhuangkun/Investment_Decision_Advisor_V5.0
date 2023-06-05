#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import requests
import time
import json
import threading
import datetime

import sys
sys.path.append("..")
import parsers.disguise as disguise
import log.custom_logger as custom_logger
import database.db_operator as db_operator
import data_collector.collector_tool_to_distinguish_stock_market as collector_tool_to_distinguish_stock_market
import db_mapper.target_pool.investment_target_mapper as investment_target_mapper

class CollectIndexWeightFromCNIndexInterface:
    # 从国证指数官网获取指数成分股及其权重

    def __init__(self):
        pass

    def call_interface_to_get_index_weight(self, index_id, month, header, proxy):
        # 调用国证指数公司接口获取指数成分股及权重
        # index_id: 指数代码（6位数字， 如 399396）
        # month，所查月份，如 2021-12
        # header，伪装的UA
        # proxy，伪装的IP
        # 返回 tuple (截止日期, [成份股代码，名称，上市地，交易所代码，权重])
        # 如 ( '2021-09-24', [['600519', '贵州茅台','sh','XSHG',15.19'], ['600887', '伊利股份','sh','XSHG',10.37'], ,,,,])

        # 地址模板
        #page_address = 'http://www.cnindex.com.cn/sample-detail/detail?indexcode='+index_id+'&dateStr='+current_month+'&pageNum=1&rows=1000'
        page_address = 'http://www.cnindex.com.cn/sample-detail/detail?indexcode=' + index_id + '&dateStr='+month+'&pageNum=1&rows=1000'

        # 返回成份股信息，成份股代码，名称，权重
        # [['600519', '贵州茅台','sh','XSHG',15.19'], ['600887', '伊利股份','sh','XSHG',10.37'], ,,,,]
        stocks_detail_info_list = []

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
                                    timeout=10).text
            # 转换成字典数据
            data_json = json.loads(raw_page)
            # 如果国证还没更新本月的最新数据
            # 如果接口获取到数据内容为空，数据量也为0的话，直接返空
            if(data_json["data"]==None and data_json["total"]==0):
                return None,None
            # 获取更新日期
            update_date = data_json['data']['rows'][1]['dateStr']
            # 遍历成分股权重列表
            for stock_info in data_json['data']['rows']:
                # 单个成份股的关键信息，如 ['600519', '贵州茅台','sh','XSHG',15.19']
                stock_detail_info_list = []
                # 获取股票代码
                stock_detail_info_list.append(stock_info['seccode'])
                # 将名称中间的空格替换
                stock_detail_info_list.append(stock_info['secname'].replace(' ',''))
                # 通过股票代码获取，上市地，股票交易所代码, 返回如 sh, XSHG
                market_init, market_code = collector_tool_to_distinguish_stock_market.CollectorToolToDistinguishStockMarket().distinguishStockMarketByCode(stock_info['seccode'])
                stock_detail_info_list.append(market_init)
                stock_detail_info_list.append(market_code)
                # 权重
                stock_detail_info_list.append(stock_info['weight'])
                stocks_detail_info_list.append(stock_detail_info_list)

            # 按成分股股票代码从大到小排序
            stocks_detail_info_list.sort(key=lambda x: x[0], reverse=True)
            # 返回如 ('2021-12-13', [['600519', '贵州茅台', 'sh', 'XSHG', 15.19], ['000858', '五粮液', 'sz', 'XSHE', 15.18],,,,,])
            return update_date, stocks_detail_info_list


        # 如果读取超时，重新在执行一遍解析页面
        except requests.exceptions.ReadTimeout:
            # 日志记录
            msg = "从国证指数官网" + page_address + '  ' + "获取最新成份股权重信息 " + " ReadTimeout。" + " 即将重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_index_latest_constituent_stock_and_weight(index_id)

        # 如果连接请求超时，重新在执行一遍解析页面
        except requests.exceptions.ConnectTimeout:
            # 日志记录
            msg = "从国证指数官网" + page_address + '  ' + "获取最新成份股权重信息 " + " ConnectTimeout。" + " 即将重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_index_latest_constituent_stock_and_weight(index_id)

        # 如果请求超时，重新在执行一遍解析页面
        except requests.exceptions.Timeout:
            # 日志记录
            msg = "从国证指数官网" + page_address + '  ' + "获取最新成份股权重信息 " + " Timeout。" + " 即将重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_index_latest_constituent_stock_and_weight(index_id)

        except Exception as e:
            # 日志记录
            msg = "从国证指数官网" + page_address + '  ' + "获取最新成份股权重信息 " + str(e)+ " 即将重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_index_latest_constituent_stock_and_weight(index_id)

    def get_single_index_latest_constituent_stock_and_weight(self, index_id):
        # 从国证官网获取单个指数最新成份股和权重信息
        # index_id: 指数代码（6位数字， 如 399396）
        # 返回 tuple (截止日期, [成份股代码，名称，上市地，交易所代码，权重])
        # 如 ( '2021-09-24', [['600519', '贵州茅台','sh','XSHG',15.19'], ['600887', '伊利股份','sh','XSHG',10.37'], ,,,,])

        # 伪装，隐藏UA和IP
        header, proxy = disguise.Disguise().assemble_header_proxy()

        current_month = time.strftime("%Y-%m", time.localtime())

        # 1. 获取「今天」
        today = datetime.date.today()
        # 2. 获取当前月的第一天
        first = today.replace(day=1)
        # 3. 减一天，得到上个月的最后一天
        last_month = (first - datetime.timedelta(days=1)).strftime("%Y-%m")

        # 所有需要检查的月份
        # 因为国证官网，每月可能会更新两次数据，月中旬及月末
        # 月中旬更新用本月月份检查
        # 月末更新用上月月份检查
        # 优先检查本月的
        months = [current_month,last_month]

        for month in months:
            update_date, stocks_detail_info_list = self.call_interface_to_get_index_weight(index_id, month, header, proxy)
            # 如果有数据返回
            if(update_date != None and stocks_detail_info_list!=None):
                return update_date, stocks_detail_info_list
        return None, None


    def get_cn_index_from_index_target(self):
        # 从标的池中获取国证公司的指数
        # 返回：指数代码及对应的指数名称的字典
        # 如 {'399396': '国证食品饮料行业', '399440': '国证钢铁',,,}

        # 存放指数代码及对应的指数名称的字典
        target_cn_index_dict = dict()

        #[{'index_code': '399396', 'index_name': '国证食品饮料行业', 'index_code_with_init': 'sz399396', 'index_code_with_market_code': '399396.XSHE'},,,]
        target_cs_index_info_list =  investment_target_mapper.InvestmentTargetMapper().get_given_index_company_index("index", "active", "buy", "国证")
        for info in target_cs_index_info_list:
            target_cn_index_dict[info["index_code"]] = info["index_name"]
        return target_cn_index_dict


    def save_index_info_into_db(self,index_code, index_name, update_date, stocks_detail_info_list):
        # 将指数成分股信息存入数据库
        # index_code，指数代码，399396
        # index_name，指数名称，国证食品饮料行业
        # update_date，更新日期，2021-12-13
        # stocks_detail_info_list，成分股的信息， [['600519', '贵州茅台','sh','XSHG',15.19'], ['600887', '伊利股份','sh','XSHG',10.37']，，，]
        # 返回：存入数据库

        for stock_info in stocks_detail_info_list:
            # 插入的SQL
            inserting_sql = "INSERT INTO index_constituent_stocks_weight(index_code,index_name," \
                            "stock_code,stock_name,stock_exchange_location,stock_market_code," \
                            "weight,source,index_company,p_day)" \
                            "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                                index_code, index_name, stock_info[0], stock_info[1], stock_info[2], stock_info[3], stock_info[4],
                                 '国证官网', '国证', update_date)
            db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)


    def check_if_saved_before(self, index_code, update_date, stocks_detail_info_list):
        # 与数据库的内容对比，是否已存过
        # index_code,指数代码，399396
        # update_date, 更新日期，2021-12-13
        # stocks_detail_info_list, 成分股的信息， [['600519', '贵州茅台','sh','XSHG',15.19'], ['600887', '伊利股份','sh','XSHG',10.37']，，，]
        # 返回：如果存储过，则返回True; 未储存过，则返回False

        # 查询sql
        selecting_sql = "select index_code, index_name, stock_code, stock_name, weight, p_day from " \
                        "index_constituent_stocks_weight where p_day = (select max(p_day) as max_day from " \
                        "index_constituent_stocks_weight where index_code = '%s' and source = '%s') and " \
                        "index_code = '%s' and source = '%s' order by stock_code desc" % (
                        index_code, '国证官网', index_code, '国证官网')
        db_index_content = db_operator.DBOperator().select_all("financial_data", selecting_sql)

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
            elif (stocks_detail_info_list[i][4] != float(db_index_content[i]["weight"])):
                return False
            # 对比发布日期是否一致
            elif (update_date != str(db_index_content[i]["p_day"])):
                return False
        return True


    def collect_cn_index_single_thread(self):
        # 单线程收集国证指数信息
        # 从标的池中获取国证公司的指数
        target_cn_index_dict = self.get_cn_index_from_index_target()
        # 遍历国证指数
        for index_code in target_cn_index_dict:
            # 获取更新日期和成分股信息
            update_date, stocks_detail_info_list = self.get_single_index_latest_constituent_stock_and_weight(index_code)
            # 指数名称
            index_name = target_cn_index_dict[index_code]
            # 检查是否储存过
            is_saved_before = self.check_if_saved_before(index_code, update_date, stocks_detail_info_list)
            # 如果储存过，则跳过
            if (is_saved_before):
                # 日志记录
                msg = index_code + " " + index_name + " 曾经储存过，无需再存储"
                custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 如果未存储过，则存入数据库
            else:
                self.save_index_info_into_db(index_code, index_name, update_date, stocks_detail_info_list)
                # 日志记录
                msg = index_code + " " + index_name + " 未储存过，已更新指数信息"
                custom_logger.CustomLogger().log_writter(msg, lev='warning')


    def get_check_and_save_index_info(self, index_code, target_cn_index_dict, threadLock):
        # 获取，检查是否存储过，并存储指数成分股及权重信息
        # index_code,指数代码，399396
        # target_cn_index_dict，指数代码及对应的指数名称的字典
        # threadLock：线程锁
        # 如 {'399396': '国证食品饮料行业', '399440': '国证钢铁',,,}

        # 获取更新日期和成分股信息
        update_date, stocks_detail_info_list = self.get_single_index_latest_constituent_stock_and_weight(index_code)
        # 指数名称
        index_name = target_cn_index_dict[index_code]

        # 检查是否储存过
        is_saved_before = self.check_if_saved_before(index_code, update_date, stocks_detail_info_list)
        # 如果储存过，则跳过
        if (is_saved_before):
            # 获取锁，用于线程同步
            threadLock.acquire()
            # 日志记录
            msg = index_code + " " + index_name + " 曾经储存过，无需再存储"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 释放锁，开启下一个线程
            threadLock.release()
        # 如果未存储过，则存入数据库
        else:
            self.save_index_info_into_db(index_code, index_name, update_date, stocks_detail_info_list)
            # 获取锁，用于线程同步
            threadLock.acquire()
            # 日志记录
            msg = index_code + " " + index_name + " 未储存过，已更新指数信息"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 释放锁，开启下一个线程
            threadLock.release()


    def collect_cn_index_multi_threads(self):
        # 多线程收集国证指数信息
        # 从标的池中获取国证公司的指数
        target_cn_index_dict = self.get_cn_index_from_index_target()
        # 启用多线程
        running_threads = []
        # 启用线程锁
        threadLock = threading.Lock()
        # 遍历国证指数
        for index_code in target_cn_index_dict:
            # 启动线程
            running_thread = threading.Thread(target=self.get_check_and_save_index_info,
                                              kwargs={"index_code": index_code,
                                                      "target_cn_index_dict": target_cn_index_dict,
                                                      "threadLock": threadLock})
            running_threads.append(running_thread)

            # 开启新线程
        for mem in running_threads:
            mem.start()

            # 等待所有线程完成
        for mem in running_threads:
            mem.join()

    def main(self):
        self.collect_cn_index_multi_threads()
        #self.collect_cn_index_single_thread()


if __name__ == '__main__':
    time_start = time.time()
    go = CollectIndexWeightFromCNIndexInterface()
    #go.main()
    #go.collect_cn_index()
    #result = go.get_single_index_latest_constituent_stock_and_weight('399396')
    #result = go.collect_all_target_cn_index_weight_single_thread()
    result = go.get_cn_index_from_index_target()
    print(result)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))