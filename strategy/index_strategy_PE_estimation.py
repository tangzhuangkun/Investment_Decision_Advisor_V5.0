#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import decimal
import time
import threading

import sys
sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger
import db_mapper.target_pool.investment_target_mapper as investment_target_mapper
import db_mapper.financial_data.mix_top10_with_bottom_no_repeat_mapper as mix_top10_with_bottom_no_repeat_mapper
import data_collector.get_target_real_time_indicator_from_interfaces as get_target_real_time_indicator_from_interfaces

"""
指数估值策略，市盈率估值法
用于非周期性行业
频率：每个交易日，盘中
"""
class IndexStrategyPEEstimation:

    def __init__(self):
        # 用于记录指数的实时市盈率
        self.index_real_time_positive_pe_ttm = 0
        # 用于记录指数累加有效（即为正值的）市盈率成分股权重
        self.index_positive_pe_ttm_weight = 0
        # 需要关注的过去x年的数据
        self._PREVIOUS_YEARS_LIST = [3, 4, 5, 8, 10]


    """
    根据成分股在指数中的权重，计算单个成分股的实时市盈率
    :param, stock_code_with_init: 股票代码（2位上市地+6位数字， 如 sz000596）
    :param, stock_weight, 在指数中，该成分股权重，如果市盈率为亏损，则置为0
    :param, threadLock, 线程锁
    """
    def get_and_calculate_single_stock_pe_ttm_weight_in_index(self, stock_code_with_init, stock_weight, threadLock):

        # 从腾讯接口获取实时估值数据
        stock_real_time_pe_ttm = get_target_real_time_indicator_from_interfaces.GetTargetRealTimeIndicatorFromInterfaces().get_single_target_real_time_indicator(
            stock_code_with_init, 'pe_ttm')
        # 如果获取的股票实时滚动市盈率小于0，即’亏损‘
        if (decimal.Decimal(stock_real_time_pe_ttm)<0):
            # 股票实时滚动市盈率为0
            stock_real_time_pe_ttm = "0"
            # 忽略亏损的成分股票的权重
            stock_weight = 0

        # 获取锁，用于线程同步
        threadLock.acquire()
        # 统计指数的实时市盈率，成分股权重*股票实时的市盈率
        self.index_real_time_positive_pe_ttm += stock_weight * decimal.Decimal(stock_real_time_pe_ttm)
        # 累加有效（即为正值的）市盈率成分股权重
        self.index_positive_pe_ttm_weight += stock_weight
        # 释放锁，开启下一个线程
        threadLock.release()

    """
    多线程计算指数的实时市盈率
    :param, index_code, 指数代码,如 399997
    :param, index_name, 指数名称,如 中证白酒
    :return，指数的实时市盈率， 如 70.5937989
    """
    def calculate_real_time_index_pe_ttm_multiple_threads(self,index_code, index_name):

        # 统计指数实时的市盈率
        self.index_real_time_positive_pe_ttm = 0
        # 统计指数实时的市盈率为正数的权重合计
        self.index_positive_pe_ttm_weight = 0

        """
        获取指数的成分股和权重
        如，[{'stock_code': '000568', 'stock_code_with_init': 'sz000568', 'stock_name': '泸州老窖',
        'weight': Decimal('15.032706411640243000'), 'stock_exchange_location': 'sz', 'stock_market_code': 'XSHE',
        'p_day': datetime.date(2022, 4, 1)},
        {'stock_code': '000596', 'stock_code_with_init': 'sz000596', 'stock_name': '古井贡酒',
        'weight': Decimal('3.038864036791435500'), 'stock_exchange_location': 'sz', 'stock_market_code': 'XSHE',
        'p_day': datetime.date(2022, 4, 1)}, ,,,]
        """
        stocks_and_their_weights = mix_top10_with_bottom_no_repeat_mapper.MixTop10WithBottomNoRepeat().get_index_constitute_stocks(index_code)

        # 启用多线程
        running_threads = []
        # 启用线程锁
        threadLock = threading.Lock()

        # 遍历指数的成分股
        for i in range(len(stocks_and_their_weights)):
            # 股票代码（2位上市地+6位数字， 如 sz000596）
            stock_code_with_init = stocks_and_their_weights[i]["stock_code_with_init"]
            # 获取成分股权重,
            stock_weight = stocks_and_their_weights[i]["weight"]

            # 启动线程
            running_thread = threading.Thread(target=self.get_and_calculate_single_stock_pe_ttm_weight_in_index,
                                              kwargs={"stock_code_with_init": stock_code_with_init, "stock_weight": stock_weight,
                                                      "threadLock": threadLock})
            running_threads.append(running_thread)

            # 开启新线程
        for mem in running_threads:
            mem.start()

            # 等待所有线程完成
        for mem in running_threads:
            mem.join()

        # 整体市盈率除以有效权重得到有效市盈率
        index_real_time_effective_pe_ttm = self.index_real_time_positive_pe_ttm / self.index_positive_pe_ttm_weight

        # 日志记录
        log_msg = '已获取 ' + index_name + ' 实时市盈率(PE_TTM)为 ' + str(round(index_real_time_effective_pe_ttm, 5))
        custom_logger.CustomLogger().log_writter(log_msg, 'info')

        # （市盈率为正值的成分股）累加市盈率/（市盈率为正值的成分股）有效权重
        return round(index_real_time_effective_pe_ttm, 5)

    """
    """
    def generate_index_PE_strategy_msg(self):

        # 获取标的指数信息
        # 如，[{'target_code': '000932', 'target_name': '中证800消费', 'target_code_with_init': 'sh000932', 'target_code_with_market_code': '000932.XSHE'},
        #{'target_code': '399997', 'target_name': '中证白酒', 'target_code_with_init': 'sz399997', 'target_code_with_market_code': '399997.XSHE'}]
        indexes_and_their_names = investment_target_mapper.InvestmentTargetMapper().get_target_valuated_by_method("index", "pe_ttm", "active", "buy")
        print(indexes_and_their_names)


if __name__ == '__main__':
    time_start = time.time()
    go = IndexStrategyPEEstimation()
    #result = go.generate_index_PE_strategy_msg()
    result = go.calculate_real_time_index_pe_ttm_multiple_threads("399997", "中证白酒")
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)