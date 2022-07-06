#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import requests
import time

import sys
sys.path.append("..")
import parsers.disguise as disguise
import log.custom_logger as custom_logger

class GetStockRealTimeIndicatorFromInterfaces:
    # 从腾讯接口获取实时估值数据
    # 1、获取实时的股票滚动市盈率,pe_ttm
    # 2、获取实时的股票市净率,pb
    # 3、获取实时的股票滚动股息率,dr_ttm

    def __init__(self):
        pass


    def get_single_stock_real_time_indicator(self, stock_id, indicator):
        '''
        解析接口信息,从接口获取实时的股票指标
        :param stock_id: 股票代码（2位上市地+6位数字， 如 sz000596）
        :param indicator: 需要抓取的指标，如 pe_ttm,市盈率TTM；pb,市净率，dr_ttm,滚动股息率 等
        :return: 获取的实时的股票滚动市盈率 格式如 32.74
        '''

        # 地址模板
        interface_address = 'https://qt.gtimg.cn/q=' + stock_id

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
            raw_data = requests.get(interface_address,  verify=False, stream=False,
                                    timeout=10).text

            data_list = raw_data.split("~")
            if indicator == "pe_ttm":
                return data_list[39]
            elif indicator == "pb":
                return data_list[46]
            elif indicator == "dr_ttm":
                return data_list[64]
            else:
                # 日志记录
                msg = "Unknown indicator"
                custom_logger.CustomLogger().log_writter(msg, lev='warning')
                # 返回 空
                return -10000

        # 如果读取超时，重新在执行一遍解析页面
        except requests.exceptions.ReadTimeout:
            # 日志记录
            msg = "Collected stock real time " + indicator + " from " + interface_address + '  ' + "ReadTimeout"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_stock_real_time_indicator(stock_id, indicator)

        # 如果连接请求超时，重新在执行一遍解析页面
        except requests.exceptions.ConnectTimeout:
            # 日志记录
            msg = "Collected stock real time " + indicator + " from " + interface_address + '  ' + "ConnectTimeout"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_stock_real_time_indicator(stock_id, indicator)

        # 如果请求超时，重新在执行一遍解析页面
        except requests.exceptions.Timeout:
            # 日志记录
            msg = "Collected stock real time " + indicator + " from " + interface_address + '  ' + "Timeout"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_stock_real_time_indicator(stock_id, indicator)

        except Exception as e:
            # 日志记录
            msg = interface_address + str(e)
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_stock_real_time_indicator(stock_id, indicator)



    """
    
    def get_single_stock_real_time_indicator(self, stock_id, header, proxy, indicator):
        '''
        解析接口信息,从接口获取实时的股票指标
        :param stock_id: 股票代码（2位上市地+6位数字， 如 sz000596）
        :param header: 伪装的UA
        :param proxy: 伪装的IP
        :param indicator: 需要抓取的指标，如 pe_ttm,市盈率TTM；pb,市净率，dr_ttm,滚动股息率 等
        :return: 获取的实时的股票滚动市盈率 格式如 32.74
        '''

        # 地址模板
        interface_address = 'https://qt.gtimg.cn/q=' + stock_id

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
            raw_data = requests.get(interface_address, headers=header, proxies=proxy, verify=False, stream=False,
                                        timeout=10).text

            data_list = raw_data.split("~")
            if indicator=="pe_ttm":
                return data_list[39]
            elif indicator=="pb":
                return data_list[46]
            elif indicator=="dr_ttm":
                return data_list[64]
            else:
                # 日志记录
                msg = "Unknown indicator"
                custom_logger.CustomLogger().log_writter(msg, lev='warning')
                # 返回 空
                return -10000

        # 如果读取超时，重新在执行一遍解析页面
        except requests.exceptions.ReadTimeout:
            # 日志记录
            msg = "Collected stock real time " + indicator + " from " + interface_address + '  ' + "ReadTimeout"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_stock_real_time_indicator(stock_id, header, proxy, indicator)

        # 如果连接请求超时，重新在执行一遍解析页面
        except requests.exceptions.ConnectTimeout:
            # 日志记录
            msg = "Collected stock real time " + indicator + " from " + interface_address + '  ' + "ConnectTimeout"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_stock_real_time_indicator(stock_id, header, proxy, indicator)

        # 如果请求超时，重新在执行一遍解析页面
        except requests.exceptions.Timeout:
            # 日志记录
            msg = "Collected stock real time " + indicator + " from " + interface_address + '  ' + "Timeout"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_stock_real_time_indicator(stock_id, header, proxy, indicator)

        except Exception as e:
            # 日志记录
            msg = interface_address + str(e)
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_stock_real_time_indicator(stock_id, header, proxy, indicator)
    
    """


    '''
        def get_single_stock_real_time_indicator(self, stock_id, indicator):
        # 从接口获取实时的股票指标
        # stock_id: 股票代码（2位上市地+6位数字， 如 sz000596）
        # indicator, 需要抓取的指标，如 pe_ttm,市盈率TTM；pb,市净率，dr_ttm,滚动股息率 等
        # 返回： 获取的实时的股票滚动市盈率 格式如 32.74

        # 伪装，隐藏UA和IP
        header, proxy = disguise.Disguise().assemble_header_proxy()

        # 暂时写死，该API对IP要求不高
        # 不调用IP代理的API来获取新的IP，可节约代理IP账户中的资源
        #header = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE'}
        #proxy =  {"http": 'http://118.190.244.234:3128', "https": 'https://118.190.244.234:3128'}

        return self.get_interface_content(stock_id, header, proxy, indicator)
    
    '''




if __name__ == '__main__':
    time_start = time.time()
    go = GetStockRealTimeIndicatorFromInterfaces()
    result = go.get_single_stock_real_time_indicator("sh600519","pe_ttm")
    print(result)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))
