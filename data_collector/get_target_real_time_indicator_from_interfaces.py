#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import sys
import time

import requests

sys.path.append("..")
import log.custom_logger as custom_logger

class GetTargetRealTimeIndicatorFromInterfaces:
    # 从接口获取实时数据

    def __init__(self):
        pass

    def get_single_target_real_time_indicator(self, stock_id, indicator):
        '''
        # 从腾讯接口获取实时估值数据
        # 参考资料，https://www.173it.cn/xitong/xinxi/3/43901.html
        # 1、获取实时的股票滚动市盈率,pe_ttm
        # 2、获取实时的股票市净率,pb
        # 3、获取实时的股票滚动股息率,dr_ttm
        # 4、获取实时的涨跌幅，change
        解析接口信息,从接口获取实时的股票指标
        :param stock_id: 股票代码（2位上市地+6位数字， 如 sz000596）
        :param indicator: 需要抓取的指标，如 pe_ttm,市盈率TTM；pb,市净率；dr_ttm,滚动股息率；change,涨跌幅 等
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
            # 香港市场接口的解析规则
            if stock_id[:2] == "hk":
                if indicator == "pe_ttm":
                    return data_list[57]
                elif indicator == "pb":
                    return data_list[58]
                elif indicator == "dr_ttm":
                    return data_list[47]
                elif indicator == "change":
                    return data_list[32]
                else:
                    # 日志记录
                    msg = "Unknown indicator"
                    custom_logger.CustomLogger().log_writter(msg, lev='warning')
                    # 返回 空
                    return -10000

            # A股市场接口的解析规则
            else:
                if indicator == "pe_ttm":
                    return data_list[39]
                elif indicator == "pb":
                    return data_list[46]
                elif indicator == "dr_ttm":
                    return data_list[64]
                elif indicator == "change":
                    return data_list[32]
                else:
                    # 日志记录
                    msg = "Unknown indicator"
                    custom_logger.CustomLogger().log_writter(msg, lev='warning')
                    # 返回 空
                    return -10000

        # 如果读取超时，重新在执行一遍解析页面
        except requests.exceptions.ReadTimeout:
            # 日志记录
            msg = "Collected target real time " + indicator + " from " + interface_address + '  ' + "ReadTimeout"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            return self.get_single_target_real_time_indicator(stock_id, indicator)

        # 如果连接请求超时，重新在执行一遍解析页面
        except requests.exceptions.ConnectTimeout:
            # 日志记录
            msg = "Collected target real time " + indicator + " from " + interface_address + '  ' + "ConnectTimeout"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的标的物指标
            return self.get_single_target_real_time_indicator(stock_id, indicator)

        # 如果请求超时，重新在执行一遍解析页面
        except requests.exceptions.Timeout:
            # 日志记录
            msg = "Collected target real time " + indicator + " from " + interface_address + '  ' + "Timeout"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的标的物指标
            return self.get_single_target_real_time_indicator(stock_id, indicator)

        except Exception as e:
            # 日志记录
            msg = interface_address + str(e)
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的标的物指标
            return self.get_single_target_real_time_indicator(stock_id, indicator)


    def get_real_time_treasury_yield(self,indicator):
        """
        从新浪接口获取国债实时收益率
        :param indicator: 需要抓取的指标，必传，如 globalbd_gcny10 ：中国10年期国债
        return :  2.8030
        """

        if(indicator!="globalbd_gcny10"):
            # 日志记录
            msg = "Unknown indicator"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回 空
            return None


        # 获取13位时间戳
        timestamp = int(time.time() * 1000)
        # 地址模板
        interface_address =  'http://hq.sinajs.cn/?rn=' + str(timestamp) + 'list=' + indicator

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
            # 配置头文件
            headers = {'referer': 'http://finance.sina.com.cn'}
            # 得到接口返回的信息
            raw_data = requests.get(interface_address, verify=False, stream=False,headers=headers,
                                    timeout=10).text
            # 解析返回数据
            data_list = raw_data.split(",")
            # 返回实时收益率
            return data_list[3]

        # 如果读取超时，重新在执行一遍解析页面
        except requests.exceptions.ReadTimeout:
            # 日志记录
            msg = " 从 " + interface_address + "获取国债实时收益率 " + indicator + '  ' + "ReadTimeout"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的国债指标
            return self.get_real_time_treasury_yield(indicator)

        # 如果连接请求超时，重新在执行一遍解析页面
        except requests.exceptions.ConnectTimeout:
            # 日志记录
            msg = " 从 " + interface_address + "获取国债实时收益率 " + indicator + '  ' + "ConnectTimeout"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的国债指标
            return self.get_real_time_treasury_yield(indicator)

        # 如果请求超时，重新在执行一遍解析页面
        except requests.exceptions.Timeout:
            # 日志记录
            msg = " 从 " + interface_address + "获取国债实时收益率 " + indicator + '  ' + "Timeout"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的国债指标
            return self.get_real_time_treasury_yield(indicator)

        except Exception as e:
            # 日志记录
            msg = interface_address + str(e)
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的国债指标
            return self.get_real_time_treasury_yield(indicator)




if __name__ == '__main__':
    time_start = time.time()
    go = GetTargetRealTimeIndicatorFromInterfaces()
    result = go.get_single_target_real_time_indicator("sz000002", "pe_ttm")
    # result = go.get_real_time_treasury_yield("globalbd_gcny10")
    print(result)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))
