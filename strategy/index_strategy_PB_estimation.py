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
指数估值策略，市净率估值法
用于周期性行业
频率：每个交易日，盘中
"""
class IndexStrategyPBEstimation:

    def __init__(self):
        # 用于记录指数的实时市净率
        self.index_real_time_positive_pb = 0
        # 用于记录指数累加有效（即为正值的）市净率成分股权重
        self.index_positive_pb_weight = 0
        # 需要关注的过去x年的数据
        self._PREVIOUS_YEARS_LIST = [3, 4, 5, 8, 10]


    """
    根据成分股在指数中的权重，计算单个成分股的实时市净率
    :param, stock_code_with_init: 股票代码（2位上市地+6位数字， 如 sz000596）
    :param, stock_weight, 在指数中，该成分股权重，如果市净率为亏损，则置为0
    :param, threadLock, 线程锁
    """
    def get_and_calculate_single_stock_pb_weight_in_index(self, stock_code_with_init, stock_weight, threadLock):

        # 从腾讯接口获取实时估值数据
        stock_real_time_pb = get_target_real_time_indicator_from_interfaces.GetTargetRealTimeIndicatorFromInterfaces().get_single_target_real_time_indicator(
            stock_code_with_init, 'pb')
        # 如果获取的股票实时滚动市净率小于0，即’亏损‘
        if (decimal.Decimal(stock_real_time_pb)<0):
            # 股票实时滚动市净率为0
            stock_real_time_pb = "0"
            # 忽略亏损的成分股票的权重
            stock_weight = 0

        # 获取锁，用于线程同步
        threadLock.acquire()
        # 统计指数的实时市净率，成分股权重*股票实时的市净率
        self.index_real_time_positive_pb += stock_weight * decimal.Decimal(stock_real_time_pb)
        # 累加有效（即为正值的）市净率成分股权重
        self.index_positive_pb_weight += stock_weight
        # 释放锁，开启下一个线程
        threadLock.release()


    """
    多线程计算指数的实时市净率
    :param, index_code, 指数代码,如 399986
    :param, index_name, 指数名称,如 中证银行
    :return，指数的实时市净率， 如 0.61084
    """
    def calculate_real_time_index_pb_multiple_threads(self,index_code, index_name):

        # 统计指数实时的市净率
        self.index_real_time_positive_pb = 0
        # 统计指数实时的市净率为正数的权重合计
        self.index_positive_pb_weight = 0


        """
        获取指数的成分股和权重
        如，[[{'stock_code': '000001', 'stock_code_with_init': 'sz000001', 'stock_name': '平安银行', 
        'weight': Decimal('4.962623718676787000'), 'stock_exchange_location': 'sz', 'stock_market_code': 'XSHE', 
        'p_day': datetime.date(2023, 6, 1)}, 
        {'stock_code': '001227', 'stock_code_with_init': 'sz001227', 'stock_name': '兰州银行', 
        'weight': Decimal('0.079000000000000000'), 'stock_exchange_location': 'sz', 'stock_market_code': 'XSHE', 
        'p_day': datetime.date(2023, 5, 31)}, , ,,,]
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
            running_thread = threading.Thread(target=self.get_and_calculate_single_stock_pb_weight_in_index,
                                              kwargs={"stock_code_with_init": stock_code_with_init, "stock_weight": stock_weight,
                                                      "threadLock": threadLock})
            running_threads.append(running_thread)

            # 开启新线程
        for mem in running_threads:
            mem.start()

            # 等待所有线程完成
        for mem in running_threads:
            mem.join()

        # 整体市净率除以有效权重得到有效市净率
        index_real_time_effective_pb = self.index_real_time_positive_pb / self.index_positive_pb_weight

        # 日志记录
        log_msg = '已获取 ' + index_name + ' 实时市净率(pb)为 ' + str(round(index_real_time_effective_pb, 5))
        custom_logger.CustomLogger().log_writter(log_msg, 'info')

        # （市净率为正值的成分股）累加市净率/（市净率为正值的成分股）有效权重
        return round(index_real_time_effective_pb, 5)

    """
    获取当前指数上一个交易日的扣商誉市净率
    :param, index_code, 指数代码,如 399986
    :return, 0.614472774
    """
    def get_last_trading_day_pb_wo_gw(self, index_code):
        # 获取当前日期
        today = time.strftime("%Y-%m-%d", time.localtime())
        # 上一个交易日日期, 如 2022-08-03
        the_last_trading_date = trading_days_mapper.TradingDaysMapper().get_the_lastest_trading_date(today)
        # 获取当前指数上一个交易日的扣商誉市净率
        last_trading_date_pb_wo_gw = index_components_historical_estimations_mapper.IndexComponentsHistoricalEstimationMapper().get_last_trading_date_pb_wo_gw(index_code, today,  the_last_trading_date)
        return last_trading_date_pb_wo_gw


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
    计算当前实时市净率和预估扣商誉市净率在历史上的百分位
    :param, index_code, 指数代码,如 399986
    :param, index_code_with_init，指数代码及上市地,如 sz399986
    :param, index_name, 指数名称,如 中证银行
    :return,  列表中第一个为值，后面为该值在对应年份的百分位
    {'pb': ['0.61', '3.7%', '2.77%', '2.22%', '1.39%', '1.11%'], 
    'pb_wo_gw': ['0.62', '3.43%', '2.57%', '2.06%', '1.28%', '1.03%']}
    """

    def cal_the_pe_percentile_in_history(self, index_code, index_code_with_init, index_name):

        # 获取当前日期
        today = time.strftime("%Y-%m-%d", time.localtime())
        # 指数实时市净率
        index_real_time_pb = self.calculate_real_time_index_pb_multiple_threads(index_code, index_name)
        # 指数上个交易日的扣商誉市净率
        index_last_trading_day_pb_wo_gw = self.get_last_trading_day_pb_wo_gw(index_code)
        # 指数最新实时涨跌幅
        change_rate = get_target_real_time_indicator_from_interfaces.GetTargetRealTimeIndicatorFromInterfaces().get_single_target_real_time_indicator(
            index_code_with_init, "change")
        # 指数当前预估扣商誉市净率
        index_estimated_pb_wo_gw = index_last_trading_day_pb_wo_gw * (100 + decimal.Decimal(change_rate)) / 100

        # 需要返回的结果
        result_dict = dict()
        # 市净率，加入指数实时pb
        result_dict["pb"] = [str(round(index_real_time_pb, 2))]
        # 扣商誉市净率，加入指数当前预估扣商誉市净率
        result_dict["pb_wo_gw"] = [str(round(index_estimated_pb_wo_gw, 2))]
        # 遍历年份列表
        for year in self._PREVIOUS_YEARS_LIST:
            # 市净率，各年份的百分位情况
            pb_list = result_dict["pb"]
            index_a_period_pb = index_components_historical_estimations_mapper.IndexComponentsHistoricalEstimationMapper().get_index_a_period_estimation(
                index_code, "pb", today, year)
            pb_percentile = self.binary_cal(index_real_time_pb, "pb", index_a_period_pb)
            pb_list.append(pb_percentile)
            result_dict["pb"] = pb_list
            # 扣商誉市净率，各年份的百分位情况
            pb_wo_gw_list = result_dict["pb_wo_gw"]
            index_a_period_pb_wo_gw = index_components_historical_estimations_mapper.IndexComponentsHistoricalEstimationMapper().get_index_a_period_estimation(
                index_code, "pb_wo_gw", today, year)
            pb_wo_gw_percentile = self.binary_cal(index_last_trading_day_pb_wo_gw,
                                                             "pb_wo_gw", index_a_period_pb_wo_gw)
            pb_wo_gw_list.append(pb_wo_gw_percentile)
            result_dict["pb_wo_gw"] = pb_wo_gw_list

        return result_dict

    """
    生成市净率策略的提示信息
    :return ,
    中证800地产(399965) 3.91%
    估值方式 | 值 | 3年 | 4年 | 5年 | 8年 | 10年
    实时市净率 | 0.84 | 1.78% | 1.34% | 1.07% | 0.67% | 0.53%
    预估扣商誉市净率 | 0.90 | 2.06% | 1.54% | 1.23% | 0.77% | 0.62%
    
    中证银行(399986) 1.22%
    估值方式 | 值 | 3年 | 4年 | 5年 | 8年 | 10年
    实时市净率 | 0.61 | 3.7% | 2.77% | 2.22% | 1.39% | 1.11%
    预估扣商誉市净率 | 0.62 | 3.43% | 2.57% | 2.06% | 1.28% | 1.03%
    """

    def generate_index_PB_strategy_msg(self):

        msg = ""
        # 获取标的指数信息
        # 如，[{'target_code': '000932', 'target_name': '中证800消费', 'target_code_with_init': 'sh000932', 'target_code_with_market_code': '000932.XSHE'},
        # {'target_code': '399997', 'target_name': '中证白酒', 'target_code_with_init': 'sz399997', 'target_code_with_market_code': '399997.XSHE'}]
        indexes_and_their_names_list = investment_target_mapper.InvestmentTargetMapper().get_target_valuated_by_method(
            "index", "pb", "active", "buy")
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
            # 展示市净率具体值,各年份百分位
            msg += "实时市净率"
            for estimation_value in index_result_dict["pb"]:
                msg += " | " + estimation_value
            msg += "\n"
            # 展示预估扣商誉市净率具体值,各年份百分位
            msg += "预估扣商誉市净率"
            for estimation_value in index_result_dict["pb_wo_gw"]:
                msg += " | " + estimation_value
            msg += "\n"
            msg += "\n"

        # 日志记录，报告信息
        custom_logger.CustomLogger().log_writter(msg, 'info')

        # 日志记录
        log_msg = '标的指数盘中市净率估值报表生成完毕'
        custom_logger.CustomLogger().log_writter(log_msg, 'info')
        return msg

    def main(self):
        return self.generate_index_PB_strategy_msg()

if __name__ == '__main__':
    time_start = time.time()
    go = IndexStrategyPBEstimation()
    result = go.main()
    #result = go.generate_index_PB_strategy_msg()
    #result = go.calculate_real_time_index_pb_multiple_threads("399986", "中证银行")
    #result = go.get_last_trading_day_pb_wo_gw("399986")
    #result = go.cal_the_pe_percentile_in_history("399986", "sz399986", "中证银行")
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)