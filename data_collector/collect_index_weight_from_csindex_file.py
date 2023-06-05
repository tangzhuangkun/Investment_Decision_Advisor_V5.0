#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
import time
import threading
import os
import xlrd

import sys
sys.path.append("..")
import parsers.disguise as disguise
import log.custom_logger as custom_logger
import database.db_operator as db_operator
import db_mapper.target_pool.investment_target_mapper as investment_target_mapper

class CollectIndexWeightFromCSIndexFile:
    # 从中证官网获取指数成分股及权重文件，并收集信息

    def __init__(self):
        # 当天的日期
        self.today = time.strftime("%Y-%m-%d", time.localtime())
        # 权重文件存放路径
        self.index_weight_samples_path = os.path.abspath(os.path.join(os.getcwd(), "../.."))+"/index_weight_samples/"


    def download_index_weight_file(self, index_code, index_name, header, proxy, threadLock):
        # 下载指数成分股及权重文件
        # index_code ： 指数代码，如 399997
        # index_name: 指数名称，如 中证白酒指数
        # header，伪装的UA
        # proxy，伪装的IP
        # threadLock, 线程锁
        # 返回：下载文件

        # 地址模板
        interface_address = 'https://csi-web-dev.oss-cn-shanghai-finance-1-pub.aliyuncs.com/static/html/csindex/public/uploads/file/autofile/closeweight/'+index_code+'closeweight.xls'

        '''
        herder = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }
        r = requests.get(interface_address, headers=herder)
        # open打开excel文件，报存为后缀为xls的文件
        fp = open("../data_collector/index_weight_samples/399997.xls", "wb")
        fp.write(r.content)
        fp.close()
        '''
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

            # 得到接口返回的信息
            file_data = requests.get(interface_address, headers=header, proxies=proxy, verify=False, stream=False,
                                    timeout=10)
            # open打开excel文件，报存为后缀为xls的文件
            fp = open(self.index_weight_samples_path+index_code+"_"+index_name+"_"+self.today+".xls", "wb")
            fp.write(file_data.content)
            fp.close()
            # 获取锁，用于线程同步
            threadLock.acquire()
            # 日志记录
            msg = "从中证指数官网下载" + index_code+" "+index_name + " 指数的权重文件成功"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 释放锁，开启下一个线程
            threadLock.release()

        # 如果读取超时，重新在执行一遍解析页面
        except requests.exceptions.ReadTimeout:
            # 获取锁，用于线程同步
            threadLock.acquire()
            # 日志记录
            msg = "从中证指数官网"+interface_address+"下载 " + index_code +" "+ index_name + "指数的权重文件失败" + "ReadTimeout。正在重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 释放锁，开启下一个线程
            threadLock.release()
            # 返回解析页面得到的股票指标
            return self.download_index_weight_file_from_cs_index(index_code,index_name, threadLock)

        # 如果连接请求超时，重新在执行一遍解析页面
        except requests.exceptions.ConnectTimeout:
            # 获取锁，用于线程同步
            threadLock.acquire()
            # 日志记录
            msg = "从中证指数官网"+interface_address+"下载 " + index_code +" "+ index_name + "指数的权重文件失败" + "ConnectTimeout。正在重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 释放锁，开启下一个线程
            threadLock.release()
            # 返回解析页面得到的股票指标
            return self.download_index_weight_file_from_cs_index(index_code,index_name, threadLock)

        # 如果请求超时，重新在执行一遍解析页面
        except requests.exceptions.Timeout:
            # 获取锁，用于线程同步
            threadLock.acquire()
            # 日志记录
            msg = "从中证指数官网"+interface_address+"下载 " + index_code +" "+ index_name + "指数的权重文件失败" + "Timeout。正在重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 释放锁，开启下一个线程
            threadLock.release()
            # 返回解析页面得到的股票指标
            return self.download_index_weight_file_from_cs_index(index_code,index_name, threadLock)

        except Exception as e:
            # 获取锁，用于线程同步
            threadLock.acquire()
            # 日志记录
            msg = "从中证指数官网"+interface_address+"下载 " + index_code +" "+ index_name + "指数的权重文件失败 " + str(e)+" 正在重试"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 释放锁，开启下一个线程
            threadLock.release()
            # 返回解析页面得到的股票指标
            return self.download_index_weight_file_from_cs_index(index_code,index_name, threadLock)

    def download_index_weight_file_from_cs_index(self,index_code, index_name, threadLock):
        # 从中证官网下载 指数成份股及权重信息的文件
        # index_code: 指数代码，如 399997
        # index_name: 指数名称，如 中证白酒指数
        # 返回： 下载文件

        # 伪装，隐藏UA和IP
        header, proxy = disguise.Disguise().assemble_header_proxy()

        return self.download_index_weight_file(index_code, index_name, header, proxy, threadLock)

    def get_cs_index_from_index_target(self):
        # 从标的池中获取中证公司的指数
        # 返回：指数代码及对应的指数名称的字典
        # {'399997': '中证白酒指数', '000932': '中证主要消费', '399965': '中证800地产', '399986': '中证银行指数', '000036': '上证主要消费行业指数'}

        # 存放指数代码及对应的指数名称的字典
        target_cs_index_dict = dict()

        #[{'index_code': '399965', 'index_name': '中证800地产', 'index_code_with_init': 'sz399965',
        # 'index_code_with_market_code': '399965.XSHE'},，，]
        target_cs_index_info_list =  investment_target_mapper.InvestmentTargetMapper().get_given_index_company_index("index", "active", "buy", "中证")
        for info in target_cs_index_info_list:
            target_cs_index_dict[info["index_code"]] = info["index_name"]
        return target_cs_index_dict


    def download_all_target_cs_index_weight_single_thread(self):
        # 单线程下载所有的标的池的中证指数权重文件
        # 返回： 下载文件

        # 从标的池中获取中证公司的指数，指数代码及对应的指数名称的字典
        # {'399997': '中证白酒指数', '000932': '中证主要消费', '399965': '中证800地产', '399986': '中证银行指数', '000036': '上证主要消费行业指数'}
        target_cs_index_dict = self.get_cs_index_from_index_target()
        # 启用线程锁
        threadLock = threading.Lock()
        for index_code in target_cs_index_dict:
            # 下载权重文件
            self.download_index_weight_file_from_cs_index(index_code, target_cs_index_dict[index_code],threadLock)

    def download_all_target_cs_index_weight_multi_threads(self):
        # 多线程下载所有的标的池的中证指数权重文件
        # 返回： 下载文件

        # 从标的池中获取中证公司的指数，指数代码及对应的指数名称的字典
        # {'399997': '中证白酒指数', '000932': '中证主要消费', '399965': '中证800地产', '399986': '中证银行指数', '000036': '上证主要消费行业指数'}
        target_cs_index_dict = self.get_cs_index_from_index_target()

        # 启用多线程
        running_threads = []
        # 启用线程锁
        threadLock = threading.Lock()
        for index_code in target_cs_index_dict:
            # 启动线程
            running_thread = threading.Thread(target=self.download_index_weight_file_from_cs_index,
                                              kwargs={"index_code": index_code,
                                                      "index_name": target_cs_index_dict[index_code],
                                                      "threadLock": threadLock})
            running_threads.append(running_thread)

        # 开启新线程
        for mem in running_threads:
            mem.start()

        # 等待所有线程完成
        for mem in running_threads:
            mem.join()

    def get_all_sample_files_name(self):
        # 获取全部指数权重文件存放路径下的文件
        # 返回：全部文件名称
        # 如 ['000036_上证主要消费行业指数_2021-12-17.xls', '399997_中证白酒指数_2021-12-17.xls', ，，，]
        for root, dirs, files in os.walk(self.index_weight_samples_path):
            return files

    def the_sample_file_names_that_expected_to_be_collected(self):
        # 预计被下载的并生成的文件名称
        # 返回 预计被下载的并生成的文件名称列表
        # 如 ['399997_中证白酒指数_2021-12-18.xls', '000932_中证主要消费_2021-12-18.xls', '399965_中证800地产_2021-12-18.xls', '399986_中证银行指数_2021-12-18.xls', '000036_上证主要消费行业指数_2021-12-18.xls']

        # 预计被下载的并生成的文件名称列表
        # 如 ['399997_中证白酒指数_2021-12-18.xls', '000932_中证主要消费_2021-12-18.xls', '399965_中证800地产_2021-12-18.xls', '399986_中证银行指数_2021-12-18.xls', '000036_上证主要消费行业指数_2021-12-18.xls']
        expected_file_name_list = []

        # 从标的池中获取中证公司的指数，指数代码及对应的指数名称的字典
        # {'399997': '中证白酒指数', '000932': '中证主要消费', '399965': '中证800地产', '399986': '中证银行指数', '000036': '上证主要消费行业指数'}
        target_cs_index_dict = self.get_cs_index_from_index_target()
        for index_code in target_cs_index_dict:
            index_name = target_cs_index_dict[index_code]
            expected_file_name_list.append(index_code+"_"+index_name+"_"+self.today+'.xls')
        return expected_file_name_list

    def read_single_file_content(self, file_name):
        # 读取文件内容，按成分股股票代码从大到小排序
        # file_name, 文件名称，如 399997_中证白酒指数_2021-12-18.xls
        # 返回： list[list[]], 按成分股股票代码从大到小排序
        # 如： [['2021-11-30', '399997', '中证白酒', '600809', '山西汾酒', 'sh', 'XSHG', 15.983], ['2021-11-30', '399997', '中证白酒', '600519', '贵州茅台', 'sh', 'XSHG', 15.619], ['2021-11-30', '399997', '中证白酒', '000568', '泸州老窖', 'sz', 'XSHE', 14.922], ['2021-11-30', '399997', '中证白酒', '000858', '五 粮 液', 'sz', 'XSHE', 12.799], ['2021-11-30', '399997', '中证白酒', '002304', '洋河股份', 'sz', 'XSHE', 12.586], ['2021-11-30', '399997', '中证白酒', '000799', '酒鬼酒', 'sz', 'XSHE', 6.119], ['2021-11-30', '399997', '中证白酒', '603369', '今世缘', 'sh', 'XSHG', 4.241], ['2021-11-30', '399997', '中证白酒', '000596', '古井贡酒', 'sz', 'XSHE', 3.725], ['2021-11-30', '399997', '中证白酒', '600779', '水井坊', 'sh', 'XSHG', 3.05], ['2021-11-30', '399997', '中证白酒', '603589', '口子窖', 'sh', 'XSHG', 2.853], ['2021-11-30', '399997', '中证白酒', '000860', '顺鑫农业', 'sz', 'XSHE', 1.997], ['2021-11-30', '399997', '中证白酒', '603198', '迎驾贡酒', 'sh', 'XSHG', 1.963], ['2021-11-30', '399997', '中证白酒', '600559', '老白干酒', 'sh', 'XSHG', 1.718], ['2021-11-30', '399997', '中证白酒', '600199', '金种子酒', 'sh', 'XSHG', 0.906], ['2021-11-30', '399997', '中证白酒', '600197', '伊力特', 'sh', 'XSHG', 0.853], ['2021-11-30', '399997', '中证白酒', '603919', '金徽酒', 'sh', 'XSHG', 0.665]]


        # 读取存储excel中每行的信息，按权重从大到小排序
        # [['2021-11-30', '399997', '中证白酒', '600809', '山西汾酒', 'sh', 'XSHG', 15.983],
        #   ['2021-11-30', '399997', '中证白酒', '600519', '贵州茅台', 'sh', 'XSHG', 15.619],,,]
        file_content_list = []

        # 从文件名解析出指数名称
        # 保持整个项目中，指数名称的使用保持一致
        index_name = file_name.split("_")[1]

        # 打开xls文件,xlrd用于读取xld
        workbook = xlrd.open_workbook(self.index_weight_samples_path+file_name)
        # 打开第一张表
        sheet = workbook.sheet_by_index(0)
        # 逐行遍历
        # 表头：日期Date	指数代码 Index Code	指数名称 Index Name	指数英文名称Index Name(Eng)	成分券代码Constituent Code	成分券名称Constituent Name	成分券英文名称Constituent Name(Eng)	交易所Exchange	交易所英文名称Exchange(Eng)	权重(%)weight
        # 从第二行开始
        for r in range(1, sheet.nrows):
            row_content_list = []
            for c in range(sheet.ncols):
                cell = sheet.row_values(r)[c]
                if c==0:
                    # 业务日期
                    p_day = sheet.row_values(r)[c][:4]+'-'+sheet.row_values(r)[c][4:6]+'-'+sheet.row_values(r)[c][6:8]
                    row_content_list.append(p_day)
                elif c==1:
                    # 指数代码
                    index_code = sheet.row_values(r)[c]
                    row_content_list.append(index_code)
                elif c==2:
                    # 指数名称
                    #index_name = sheet.row_values(r)[c]
                    row_content_list.append(index_name)
                elif c==4:
                    # 股票代码
                    stock_code = sheet.row_values(r)[c]
                    row_content_list.append(stock_code)
                elif c==5:
                    # 股票名称
                    stock_name = sheet.row_values(r)[c].replace(" ","")
                    row_content_list.append(stock_name)
                elif c==8:
                    # 股票上市地
                    # 股票上市地市场代码
                    if "Shanghai" in sheet.row_values(r)[c]:
                        stock_exchange_location = 'sh'
                        sotck_market_code = 'XSHG'
                        row_content_list.append(stock_exchange_location)
                        row_content_list.append(sotck_market_code)
                    elif "Shenzhen" in sheet.row_values(r)[c]:
                        stock_exchange_location = 'sz'
                        sotck_market_code = 'XSHE'
                        row_content_list.append(stock_exchange_location)
                        row_content_list.append(sotck_market_code)
                elif c==9:
                    # 权重
                    weight = sheet.row_values(r)[c]
                    row_content_list.append(weight)
            file_content_list.append(row_content_list)
        # 按成分股代码从大到小排序
        file_content_list.sort(key=lambda x: x[3], reverse=True)

        return file_content_list

    def save_file_content_into_db(self, file_content_list):
        # 将文件内容存入数据库
        # file_content_list, 按成分股股票代码从大到小排序
        # 如： [['2021-11-30', '399997', '中证白酒', '600809', '山西汾酒', 'sh', 'XSHG', 15.983], ['2021-11-30', '399997', '中证白酒', '600519', '贵州茅台', 'sh', 'XSHG', 15.619], ['2021-11-30', '399997', '中证白酒', '000568', '泸州老窖', 'sz', 'XSHE', 14.922], ['2021-11-30', '399997', '中证白酒', '000858', '五 粮 液', 'sz', 'XSHE', 12.799], ['2021-11-30', '399997', '中证白酒', '002304', '洋河股份', 'sz', 'XSHE', 12.586], ['2021-11-30', '399997', '中证白酒', '000799', '酒鬼酒', 'sz', 'XSHE', 6.119], ['2021-11-30', '399997', '中证白酒', '603369', '今世缘', 'sh', 'XSHG', 4.241], ['2021-11-30', '399997', '中证白酒', '000596', '古井贡酒', 'sz', 'XSHE', 3.725], ['2021-11-30', '399997', '中证白酒', '600779', '水井坊', 'sh', 'XSHG', 3.05], ['2021-11-30', '399997', '中证白酒', '603589', '口子窖', 'sh', 'XSHG', 2.853], ['2021-11-30', '399997', '中证白酒', '000860', '顺鑫农业', 'sz', 'XSHE', 1.997], ['2021-11-30', '399997', '中证白酒', '603198', '迎驾贡酒', 'sh', 'XSHG', 1.963], ['2021-11-30', '399997', '中证白酒', '600559', '老白干酒', 'sh', 'XSHG', 1.718], ['2021-11-30', '399997', '中证白酒', '600199', '金种子酒', 'sh', 'XSHG', 0.906], ['2021-11-30', '399997', '中证白酒', '600197', '伊力特', 'sh', 'XSHG', 0.853], ['2021-11-30', '399997', '中证白酒', '603919', '金徽酒', 'sh', 'XSHG', 0.665]]
        # 返回：内容存入数据库

        for row_content in file_content_list:

            # 插入的SQL
            inserting_sql = "INSERT INTO index_constituent_stocks_weight(index_code,index_name," \
                            "stock_code,stock_name,stock_exchange_location,stock_market_code," \
                            "weight,source,index_company,p_day)" \
                            "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                                row_content[1], row_content[2], row_content[3], row_content[4], row_content[5],
                                row_content[6], row_content[7], '中证权重文件', '中证', row_content[0])
            db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)

    def check_if_saved_before(self, index_code, file_content_list):
        # 与数据库的内容对比，是否已存过
        # file_content_list, 按成分股股票代码从大到小排序
        # 如： [['2021-11-30', '399997', '中证白酒', '600809', '山西汾酒', 'sh', 'XSHG', 15.983], ['2021-11-30', '399997', '中证白酒', '600519', '贵州茅台', 'sh', 'XSHG', 15.619], ['2021-11-30', '399997', '中证白酒', '000568', '泸州老窖', 'sz', 'XSHE', 14.922], ['2021-11-30', '399997', '中证白酒', '000858', '五 粮 液', 'sz', 'XSHE', 12.799], ['2021-11-30', '399997', '中证白酒', '002304', '洋河股份', 'sz', 'XSHE', 12.586], ['2021-11-30', '399997', '中证白酒', '000799', '酒鬼酒', 'sz', 'XSHE', 6.119], ['2021-11-30', '399997', '中证白酒', '603369', '今世缘', 'sh', 'XSHG', 4.241], ['2021-11-30', '399997', '中证白酒', '000596', '古井贡酒', 'sz', 'XSHE', 3.725], ['2021-11-30', '399997', '中证白酒', '600779', '水井坊', 'sh', 'XSHG', 3.05], ['2021-11-30', '399997', '中证白酒', '603589', '口子窖', 'sh', 'XSHG', 2.853], ['2021-11-30', '399997', '中证白酒', '000860', '顺鑫农业', 'sz', 'XSHE', 1.997], ['2021-11-30', '399997', '中证白酒', '603198', '迎驾贡酒', 'sh', 'XSHG', 1.963], ['2021-11-30', '399997', '中证白酒', '600559', '老白干酒', 'sh', 'XSHG', 1.718], ['2021-11-30', '399997', '中证白酒', '600199', '金种子酒', 'sh', 'XSHG', 0.906], ['2021-11-30', '399997', '中证白酒', '600197', '伊力特', 'sh', 'XSHG', 0.853], ['2021-11-30', '399997', '中证白酒', '603919', '金徽酒', 'sh', 'XSHG', 0.665]]
        # 返回： 如果存储过，则返回True; 未储存过，则返回False

        # 查询sql
        selecting_sql = "select index_code, index_name, stock_code, stock_name, weight, p_day from " \
                        "index_constituent_stocks_weight where p_day = (select max(p_day) as max_day from " \
                        "index_constituent_stocks_weight where index_code = '%s' and source = '%s') and " \
                        "index_code = '%s' and source = '%s' order by stock_code desc" % (index_code,'中证权重文件',index_code,'中证权重文件')

        # 从数据库获取内容
        # [{'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '600809', 'stock_name': '山西汾酒', 'weight': Decimal('15.983000000000000000'), 'p_day': datetime.date(2021, 11, 30)}, {'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '600519', 'stock_name': '贵州茅台', 'weight': Decimal('15.619000000000000000'), 'p_day': datetime.date(2021, 11, 30)}, {'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '000568', 'stock_name': '泸州老窖', 'weight': Decimal('14.922000000000000000'), 'p_day': datetime.date(2021, 11, 30)}, {'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '000858', 'stock_name': '五 粮 液', 'weight': Decimal('12.799000000000000000'), 'p_day': datetime.date(2021, 11, 30)}, {'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '002304', 'stock_name': '洋河股份', 'weight': Decimal('12.586000000000000000'), 'p_day': datetime.date(2021, 11, 30)}, {'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '000799', 'stock_name': '酒鬼酒', 'weight': Decimal('6.119000000000000000'), 'p_day': datetime.date(2021, 11, 30)}, {'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '603369', 'stock_name': '今世缘', 'weight': Decimal('4.241000000000000000'), 'p_day': datetime.date(2021, 11, 30)}, {'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '000596', 'stock_name': '古井贡酒', 'weight': Decimal('3.725000000000000000'), 'p_day': datetime.date(2021, 11, 30)}, {'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '600779', 'stock_name': '水井坊', 'weight': Decimal('3.050000000000000000'), 'p_day': datetime.date(2021, 11, 30)}, {'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '603589', 'stock_name': '口子窖', 'weight': Decimal('2.853000000000000000'), 'p_day': datetime.date(2021, 11, 30)}, {'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '000860', 'stock_name': '顺鑫农业', 'weight': Decimal('1.997000000000000000'), 'p_day': datetime.date(2021, 11, 30)}, {'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '603198', 'stock_name': '迎驾贡酒', 'weight': Decimal('1.963000000000000000'), 'p_day': datetime.date(2021, 11, 30)}, {'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '600559', 'stock_name': '老白干酒', 'weight': Decimal('1.718000000000000000'), 'p_day': datetime.date(2021, 11, 30)}, {'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '600199', 'stock_name': '金种子酒', 'weight': Decimal('0.906000000000000000'), 'p_day': datetime.date(2021, 11, 30)}, {'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '600197', 'stock_name': '伊力特', 'weight': Decimal('0.853000000000000000'), 'p_day': datetime.date(2021, 11, 30)}, {'index_code': '399997', 'index_name': '中证白酒', 'stock_code': '603919', 'stock_name': '金徽酒', 'weight': Decimal('0.665000000000000000'), 'p_day': datetime.date(2021, 11, 30)}]
        db_index_content = db_operator.DBOperator().select_all("financial_data", selecting_sql)

        # 文件内容中的成分股个数
        file_content_len = len(file_content_list)
        # 数据库中的指数成分股个数
        db_index_content_len = len(db_index_content)
        # 对比文件内容中的成分股 与 数据库中的指数成分股 个数是否一致
        if(file_content_len!=db_index_content_len):
            return False

        for i in range(file_content_len):

            # 对比股票代码是否一致
            if (file_content_list[i][3] != db_index_content[i]["stock_code"]):
                return False
            # 对比股票权重是否一致
            elif (file_content_list[i][7] != float(db_index_content[i]["weight"])):
                return False
            # 对比发布日期是否一致
            elif (file_content_list[i][0] != str(db_index_content[i]["p_day"])):
                return False
        return True

    def read_and_save_the_all_expected_sample_files_content(self):
        # 读取并存储所有的预计下载文件内容

        # 获取全部指数权重文件存放路径下的文件名称
        all_saved_files_name_list = self.get_all_sample_files_name()
        # 预计被下载的并生成的文件名称
        expected_to_be_collected_file_name_list = self.the_sample_file_names_that_expected_to_be_collected()

        for file_name_str in expected_to_be_collected_file_name_list:
            # 如果存放路径下也包含了该文件名称
            if file_name_str  in all_saved_files_name_list:
                # 读取文件中的内容，并按成分股股票代码，从大到小排列
                file_content_list = self.read_single_file_content(file_name_str)
                # 获取指数代码
                index_code = file_name_str.split("_")[0]
                # 获取指数名称
                index_name = file_name_str.split("_")[1]
                # 文件中的内容与数据库中该指数的储存内容对比，检查是否存储过
                is_saved_before = self.check_if_saved_before(index_code,file_content_list)
                # 如果储存过，则跳过
                if(is_saved_before):
                    # 日志记录
                    msg = index_code +" "+ index_name + " 曾经储存过，无需再存储"
                    custom_logger.CustomLogger().log_writter(msg, lev='warning')
                    continue
                # 如果未存储过，则存入数据库
                else:
                    self.save_file_content_into_db(file_content_list)
                    # 日志记录
                    msg = index_code + " " + index_name + " 未储存过，已更新指数信息"
                    custom_logger.CustomLogger().log_writter(msg, lev='warning')

            # 如果存放路径下未包含该文件名称
            else:
                # 日志记录
                msg = "读取 "+file_name_str+" 文件失败，从中证指数官网下载该指数权重文件失败"
                custom_logger.CustomLogger().log_writter(msg, lev='warning')

    def main(self):
        self.download_all_target_cs_index_weight_multi_threads()
        self.read_and_save_the_all_expected_sample_files_content()

if __name__ == '__main__':
    time_start = time.time()
    go = CollectIndexWeightFromCSIndexFile()
    #go.main()
    #result = go.check_if_saved_before('399997', file_content_list)
    #print(result)
    #go.download_all_target_cs_index_weight_multi_threads()
    #go.read_and_save_the_all_expected_sample_files_content()
    #result = go.read_single_file_content('399997_中证白酒_2021-12-18.xls')
    #result = go.the_sample_file_names_that_expected_to_be_collected()
    #print(result)
    result = go.get_cs_index_from_index_target()
    print(result)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))
