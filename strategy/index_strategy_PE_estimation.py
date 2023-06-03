#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import decimal
import time
import threading

import sys
sys.path.append("..")
import log.custom_logger as custom_logger
import db_mapper.target_pool.investment_target_mapper as investment_target_mapper
import db_mapper.financial_data.mix_top10_with_bottom_no_repeat_mapper as mix_top10_with_bottom_no_repeat_mapper
import data_collector.get_target_real_time_indicator_from_interfaces as get_target_real_time_indicator_from_interfaces
import db_mapper.aggregated_data.index_components_historical_estimations_mapper as index_components_historical_estimations_mapper
import db_mapper.financial_data.trading_days_mapper as trading_days_mapper


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
    获取当前指数上一个交易日的扣非市盈率
    :param, index_code, 指数代码,如 399997
    :return, 37.876627951
    """
    def get_last_trading_day_pe_ttm_nonrecurring(self, index_code):
        # 获取当前日期
        today = time.strftime("%Y-%m-%d", time.localtime())
        # 上一个交易日日期, 如 2022-08-03
        the_last_trading_date = trading_days_mapper.TradingDaysMapper().get_the_lastest_trading_date(today)
        # 获取当前指数上一个交易日的扣非市盈率
        last_trading_date_pe_ttm_nonrecurring = index_components_historical_estimations_mapper.IndexComponentsHistoricalEstimationMapper().get_last_trading_date_pe_ttm_nonrecurring(index_code, today,  the_last_trading_date)
        return last_trading_date_pe_ttm_nonrecurring


    """
    二分法计算该值，与列表中值对比后，所处的百分位
    :param, target， 值, 28.8976
    :param, value_list， 一组值，如 [{'index_code': '399997', 'index_name': '中证白酒', 'pe_ttm': Decimal('21.825887910'), 'p_day': datetime.date(2018, 10, 30)}, 
            {'index_code': '399997', 'index_name': '中证白酒', 'pe_ttm': Decimal('22.049658433'), 'p_day': datetime.date(2018, 10, 29)}, ,,,]
    :return,  68.56%
    """
    def binary_cal(self, target, estimation_method, value_list):
        left = 0
        right = len(value_list) - 1

        # 如果比最小值还小
        if(target<=value_list[left][estimation_method]):
            return "0%"
        # 如果比最大值还大
        elif (target>=value_list[right][estimation_method]):
            return "100%"

        while left <= right:
            # 二分思路
            mid = left + (right - left) // 2
            # 中间值
            mid_value = value_list[mid][estimation_method]
            # 目标值在右侧
            if mid_value < target:
                left = mid + 1
            # 目标值在左侧
            elif mid_value > target:
                right = mid - 1
            # 恰好是目标值序号所处百分比
            else:
                return str(round((mid+1)*100/len(value_list),2)) + "%"
        # 没有完全一样的值，返回偏小值序号+1所处百分比
        return str(round((left+1)*100/len(value_list),2)) + "%"

    """
    计算当前实时滚动市盈率和预估扣非市盈率在历史上的百分位
    :param, index_code, 指数代码,如 399997
    :param, index_code_with_init，指数代码及上市地,如 sz399997
    :param, index_name, 指数名称,如 中证白酒
    :return,  列表中第一个为值，后面为该值在对应年份的百分位
    {'pe_ttm': ['28.51', '0.82%', '2.37%', '7.74%', '4.83%', '16.61%'], 
    'pe_ttm_nonrecurring': ['29.51', '0.69%', '3.09%', '14.17%', '8.83%', '19.82%']}
    
    """
    def cal_the_pe_percentile_in_history(self, index_code, index_code_with_init, index_name):

        # 获取当前日期
        today = time.strftime("%Y-%m-%d", time.localtime())
        # 指数实时pe_ttm
        index_real_time_pe_ttm = self.calculate_real_time_index_pe_ttm_multiple_threads(index_code, index_name)
        # 指数上个交易日的扣非市盈率
        index_last_trading_day_pe_ttm_nonrecurring = self.get_last_trading_day_pe_ttm_nonrecurring(index_code)
        # 指数最新实时涨跌幅
        change_rate = get_target_real_time_indicator_from_interfaces.GetTargetRealTimeIndicatorFromInterfaces().get_single_target_real_time_indicator(index_code_with_init,"change")
        # 指数当前预估扣非市盈率
        index_estimated_pe_ttm_nonrecurring = index_last_trading_day_pe_ttm_nonrecurring * (100+decimal.Decimal(change_rate)) / 100

        # 需要返回的结果
        result_dict = dict()
        # 滚动市盈率，加入指数实时pe_ttm
        result_dict["pe_ttm"] = [str(round(index_real_time_pe_ttm,2))]
        # 扣非市盈率，加入指数当前预估扣非市盈率
        result_dict["pe_ttm_nonrecurring"] = [str(round(index_estimated_pe_ttm_nonrecurring,2))]
        # 遍历年份列表
        for year in self._PREVIOUS_YEARS_LIST:
            # 滚动市盈率，各年份的百分位情况
            pe_ttm_list = result_dict["pe_ttm"]
            index_a_period_pe_ttm = index_components_historical_estimations_mapper.IndexComponentsHistoricalEstimationMapper().get_index_a_period_estimation(index_code, "pe_ttm", today, year)
            pe_ttm_percentile = self.binary_cal(index_real_time_pe_ttm, "pe_ttm", index_a_period_pe_ttm )
            pe_ttm_list.append(pe_ttm_percentile)
            result_dict["pe_ttm"] = pe_ttm_list
            # 扣非滚动市盈率，各年份的百分位情况
            pe_ttm_nonrecurring_list = result_dict["pe_ttm_nonrecurring"]
            index_a_period_pe_ttm_nonrecurring = index_components_historical_estimations_mapper.IndexComponentsHistoricalEstimationMapper().get_index_a_period_estimation(index_code, "pe_ttm_nonrecurring", today, year)
            pe_ttm_nonrecurring_percentile = self.binary_cal(index_last_trading_day_pe_ttm_nonrecurring, "pe_ttm_nonrecurring", index_a_period_pe_ttm_nonrecurring)
            pe_ttm_nonrecurring_list.append(pe_ttm_nonrecurring_percentile)
            result_dict["pe_ttm_nonrecurring"] = pe_ttm_nonrecurring_list

        return result_dict


    """
    生成市盈率策略的提示信息
    :return ,
    中证800消费(000932) 0.57%
    估值方式 | 值 | 3年 | 4年 | 5年 | 8年 | 10年
    实时滚动市盈率 | 29.68 | 0.27% | 0.21% | 7.49% | 14.84% | 23.51%
    预估扣非滚动市盈率 | 36.57 | 1.1% | 1.13% | 14.49% | 24.44% | 30.58%
    
    中证白酒(399997) 0.62%
    估值方式 | 值 | 3年 | 4年 | 5年 | 8年 | 10年
    实时滚动市盈率 | 28.14 | 0.68% | 2.16% | 5.68% | 3.54% | 15.58%
    预估扣非滚动市盈率 | 29.30 | 0.96% | 2.57% | 13.99% | 8.73% | 19.73%
    """
    def generate_index_PE_strategy_msg(self):

        msg = ""
        # 获取标的指数信息
        # 如，[{'target_code': '000932', 'target_name': '中证800消费', 'target_code_with_init': 'sh000932', 'target_code_with_market_code': '000932.XSHE'},
        #{'target_code': '399997', 'target_name': '中证白酒', 'target_code_with_init': 'sz399997', 'target_code_with_market_code': '399997.XSHE'}]
        indexes_and_their_names_list = investment_target_mapper.InvestmentTargetMapper().get_target_valuated_by_method("index", "pe_ttm", "active", "buy")
        for index_info in indexes_and_their_names_list:
            # 指数代码
            index_code = index_info['target_code']
            # 指数上市地+代码， sh000932
            index_code_with_init = index_info['target_code_with_init']
            # 指数名称
            index_name = index_info['target_name']
            # 指数最新实时涨跌幅
            change_rate = get_target_real_time_indicator_from_interfaces.GetTargetRealTimeIndicatorFromInterfaces().get_single_target_real_time_indicator(
                index_code_with_init, "change")
            index_result_dict = self.cal_the_pe_percentile_in_history(index_code, index_code_with_init, index_name)
            msg += index_name + "(" + index_code + ")" + " " + change_rate + "%" + "\n"
            # 过去X年的展示列表
            msg += "估值方式 | " + "值"
            for year_num in self._PREVIOUS_YEARS_LIST:
                msg += " | " + str(year_num) + "年"
            msg += "\n"
            # 展示滚动市盈率具体值,各年份百分位
            msg += "实时滚动市盈率"
            for estimation_value in  index_result_dict["pe_ttm"]:
                msg += " | " + estimation_value
            msg += "\n"
            # 展示扣非滚动市盈率具体值,各年份百分位
            msg += "预估扣非滚动市盈率"
            for estimation_value in index_result_dict["pe_ttm_nonrecurring"]:
                msg += " | " + estimation_value
            msg += "\n"
            msg += "\n"

        # 日志记录，报告信息
        custom_logger.CustomLogger().log_writter(msg, 'info')

        # 日志记录
        log_msg = '标的指数盘中市盈率估值报表生成完毕'
        custom_logger.CustomLogger().log_writter(log_msg, 'info')
        return msg

    def main(self):
        return self.generate_index_PE_strategy_msg()


if __name__ == '__main__':
    time_start = time.time()
    go = IndexStrategyPEEstimation()
    result = go.generate_index_PE_strategy_msg()
    #result = go.calculate_real_time_index_pe_ttm_multiple_threads("399997", "中证白酒")
    #result = go.get_last_trading_day_pe_ttm_nonrecurring("399997")
    #result = go.cal_the_pe_percentile_in_history("399997", "sz399997", "中证白酒")
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)