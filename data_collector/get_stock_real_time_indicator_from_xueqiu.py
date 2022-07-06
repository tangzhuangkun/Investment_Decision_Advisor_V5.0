#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

from bs4 import BeautifulSoup
import requests
import time

import sys
sys.path.append("..")
import parsers.disguise as disguise
import log.custom_logger as custom_logger


class GetStockRealTimeIndicatorFromXueqiu:
    # 获取雪球上股票估值数据
    # 1、获取实时的股票滚动市盈率
    # 2、获取实时的股票市净率
    # 3、获取实时的股票滚动股息率

    def __init__(self):
        pass

    def parse_page_content(self, stock_id, header, proxy, indicator):
        # 解析雪球网页信息
        # stock_id: 股票代码（2位上市地+6位数字， 如 sz000596）
        # page_address，地址
        # header，伪装的UA
        # proxy，伪装的IP
        # indicator, 需要抓取的指标，如 pe_ttm,市盈率TTM；pb,市净率，dr_ttm,滚动股息率 等
        # 返回 股票滚动市盈率

        # 地址模板
        page_address = 'https://xueqiu.com/S/' + stock_id


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

            # 使用BeautifulSoup解析页面
            bs = BeautifulSoup(raw_page, "html.parser")

            # 300开头，创业板
            if (stock_id[:5] == 'SZ300' or stock_id[:5] == 'sz300'):
                if indicator == 'pe_ttm':
                    # 解析网页信息，获取动态市盈率
                    real_time_stock_info = bs.find('table', attrs={'class': 'quote-info'})
                    tr_list = real_time_stock_info.find_all('tr')
                    real_time_pe_ttm = tr_list[4].find_all('span')[1].get_text()
                    # 日志记录
                    # msg = "Collected stock real time " + indicator + " from " + page_address
                    # custom_logger.CustomLogger().log_writter(msg, lev='debug')
                    # 返回 股票滚动市盈率
                    return real_time_pe_ttm

                elif indicator == 'pb':
                    # 解析网页信息，获取市净率
                    real_time_stock_info = bs.find('table', attrs={'class': 'quote-info'})
                    tr_list = real_time_stock_info.find_all('tr')
                    real_time_pb = tr_list[5].find_all('span')[1].get_text()
                    # 日志记录
                    # msg = "Collected stock real time " + indicator + " from " + page_address
                    # custom_logger.CustomLogger().log_writter(msg, lev='debug')
                    # 返回 股票滚动市盈率
                    return real_time_pb

                elif indicator == 'dr_ttm':
                    # 解析网页信息，获取滚动股息率
                    real_time_stock_info = bs.find('table', attrs={'class': 'quote-info'})
                    tr_list = real_time_stock_info.find_all('tr')
                    real_time_dr_ttm = tr_list[5].find_all('span')[3].get_text()
                    # 日志记录
                    # msg = "Collected stock real time " + indicator + " from " + page_address
                    # custom_logger.CustomLogger().log_writter(msg, lev='debug')
                    # 返回 股票滚动股息率
                    return real_time_dr_ttm

                else:
                    # 日志记录
                    msg = "Unknown indicator"
                    custom_logger.CustomLogger().log_writter(msg, lev='warning')
                    # 返回 空
                    return -10000

            # 688开头，科创板
            elif (stock_id[:5] == 'SH688' or stock_id[:5] == 'sh688'):
                if indicator == 'pe_ttm':
                    # 解析网页信息，获取动态市盈率
                    real_time_stock_info = bs.find('table', attrs={'class': 'quote-info'})
                    tr_list = real_time_stock_info.find_all('tr')
                    real_time_pe_ttm = tr_list[4].find_all('span')[1].get_text()
                    # 日志记录
                    # msg = "Collected stock real time " + indicator + " from " + page_address
                    # custom_logger.CustomLogger().log_writter(msg, lev='debug')
                    # 返回 股票滚动市盈率
                    return real_time_pe_ttm

                elif indicator == 'pb':
                    # 解析网页信息，获取市净率
                    real_time_stock_info = bs.find('table', attrs={'class': 'quote-info'})
                    tr_list = real_time_stock_info.find_all('tr')
                    real_time_pb = tr_list[5].find_all('span')[1].get_text()
                    # 日志记录
                    # msg = "Collected stock real time " + indicator + " from " + page_address
                    # custom_logger.CustomLogger().log_writter(msg, lev='debug')
                    # 返回 股票滚动市盈率
                    return real_time_pb

                elif indicator == 'dr_ttm':
                    # 解析网页信息，获取滚动股息率
                    real_time_stock_info = bs.find('table', attrs={'class': 'quote-info'})
                    tr_list = real_time_stock_info.find_all('tr')
                    real_time_dr_ttm = tr_list[5].find_all('span')[3].get_text()
                    # 日志记录
                    # msg = "Collected stock real time "+ indicator + " from " + page_address
                    # custom_logger.CustomLogger().log_writter(msg, lev='debug')
                    # 返回 股票滚动股息率
                    return real_time_dr_ttm

                else:
                    # 日志记录
                    msg = "Unknown indicator"
                    custom_logger.CustomLogger().log_writter(msg, lev='warning')
                    # 返回 空
                    return -10000

            # 沪A，深A，中小板
            else:
                if indicator == 'pe_ttm':
                    # 解析网页信息，获取动态市盈率
                    real_time_stock_info = bs.find('table', attrs={'class': 'quote-info'})
                    tr_list = real_time_stock_info.find_all('tr')
                    real_time_pe_ttm = tr_list[2].find_all('span')[3].get_text()
                    # 日志记录
                    # msg = "Collected stock real time " + indicator + " from " + page_address
                    # custom_logger.CustomLogger().log_writter(msg, lev='debug')
                    # 返回 股票滚动市盈率
                    return real_time_pe_ttm

                elif indicator == 'pb':
                    # 解析网页信息，获取市净率
                    real_time_stock_info = bs.find('table', attrs={'class': 'quote-info'})
                    tr_list = real_time_stock_info.find_all('tr')
                    real_time_pb = tr_list[3].find_all('span')[3].get_text()
                    # 日志记录
                    # msg = "Collected stock real time " + indicator + " from " + page_address
                    # custom_logger.CustomLogger().log_writter(msg, lev='debug')
                    # 返回 股票滚动市盈率
                    return real_time_pb

                elif indicator == 'dr_ttm':
                    # 解析网页信息，获取滚动股息率
                    real_time_stock_info = bs.find('table', attrs={'class': 'quote-info'})
                    tr_list = real_time_stock_info.find_all('tr')
                    real_time_dr_ttm = tr_list[5].find_all('span')[1].get_text()
                    # 日志记录
                    # msg = "Collected stock real time "+ indicator + " from " + page_address
                    # custom_logger.CustomLogger().log_writter(msg, lev='debug')
                    # 返回 股票滚动股息率
                    return real_time_dr_ttm

                else:
                    # 日志记录
                    msg = "Unknown indicator"
                    custom_logger.CustomLogger().log_writter(msg, lev='warning')
                    # 返回 空
                    return -10000

        # 如果读取超时，重新在执行一遍解析页面
        except requests.exceptions.ReadTimeout:
            # 日志记录
            msg = "Collected stock real time "+ indicator + " from " + page_address + '  ' + "ReadTimeout"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            # return self.parse_page_content(stock_id, header, proxy, indicator)
            return self.get_single_stock_real_time_indicator(stock_id, indicator)

        # 如果连接请求超时，重新在执行一遍解析页面
        except requests.exceptions.ConnectTimeout:
            # 日志记录
            msg = "Collected stock real time "+ indicator + " from " + page_address + '  ' + "ConnectTimeout"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            # return self.parse_page_content(stock_id, header, proxy, indicator)
            return self.get_single_stock_real_time_indicator(stock_id, indicator)

        # 如果请求超时，重新在执行一遍解析页面
        except requests.exceptions.Timeout:
            # 日志记录
            msg = "Collected stock real time "+ indicator + " from " + page_address + '  ' + "Timeout"
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            # return self.parse_page_content(stock_id, header, proxy, indicator)
            return self.get_single_stock_real_time_indicator(stock_id, indicator)

        except Exception as e:
            # 日志记录
            msg = page_address + str(e)
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            # 返回解析页面得到的股票指标
            # return self.parse_page_content(stock_id, header, proxy, indicator)
            return self.get_single_stock_real_time_indicator(stock_id, indicator)


    def get_single_stock_real_time_indicator(self, stock_id, indicator):
        # 从雪球网获取实时的股票滚动市盈率pe_ttm
        # stock_id: 股票代码（2位上市地+6位数字， 如 sz000596）
        # indicator, 需要抓取的指标，如 pe_ttm,市盈率TTM；pb,市净率，dr_ttm,滚动股息率 等
        # 返回： 获取的实时的股票滚动市盈率 格式如 32.74

        # 伪装，隐藏UA和IP
        header, proxy = disguise.Disguise().assemble_header_proxy()

        return self.parse_page_content(stock_id, header, proxy, indicator)


if __name__ == '__main__':

    time_start = time.time()
    go = GetStockRealTimeIndicatorFromXueqiu()
    real_time_pe_ttm = go.get_single_stock_real_time_indicator('sh600315', 'pe_ttm')
    print(real_time_pe_ttm)
    '''
    for i in range(1000):
        real_time_pe_ttm = go.get_single_stock_real_time_pe_ttm('SH600519')
        print(real_time_pe_ttm)
        real_time_pe_ttm = go.get_single_stock_real_time_pe_ttm('SZ002505')
        print(real_time_pe_ttm)
        print()
    '''
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))