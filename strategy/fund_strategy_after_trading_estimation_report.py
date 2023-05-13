#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import decimal
import time
import sys

sys.path.append("..")
import log.custom_logger as custom_logger
import data_miner.data_miner_common_target_index_operator as data_miner_common_target_index_operator
import data_miner.data_miner_common_index_operator as data_miner_common_index_operator

"""
跟踪标的池中指数基金标的在盘后的估值情况，并生成报告
"""


class FundStrategyAfterTradingEstimationReport:

    def __init__(self):
        # 获取标的池中跟踪关注指数的指数代码，中文名称, 地点缩写+指数代码，指数代码+证券市场代码，估值方式
        #  如 [{'index_code': '399997', 'index_name': '中证白酒指数', 'index_code_with_init': 'sz399997', 'index_code_with_market_code': '399997.XSHE', 'valuation_method': 'pe_ttm'},，，，]
        self.tracking_indexes_valuation_method_and_trigger_dict = data_miner_common_target_index_operator.DataMinerCommonTargetIndexOperator().get_index_valuation_method()
        # 需要关注的过去x年的数据
        self._PREVIOUS_YEARS_LIST = [3,4,5,7,10]

    """
    生成所有指数在过去X年估值信息字典
    return： 例如  
    {'pb': [{'index_code': '399986', 'index_name': '中证银行', 'latest_date': datetime.date(2023, 5, 11), 'pb_effective': Decimal('0.6425'), 'row_num': 55, 'total_num': 729, 'percentage': 7.42, 'previous_year_num': '3'}, {'index_code': '399986', 'index_name': '中证银行', 'latest_date': datetime.date(2023, 5, 11), 'pb_wo_gw_effective': Decimal('0.6452'), 'row_num': 55, 'total_num': 729, 'percentage': 7.42, 'previous_year_num': '3'}, {'index_code': '399986', 'index_name': '中证银行', 'latest_date': datetime.date(2023, 5, 11), 'pb_effective': Decimal('0.6425'), 'row_num': 55, 'total_num': 1214, 'percentage': 4.45, 'previous_year_num': '5'}, {'index_code': '399986', 'index_name': '中证银行', 'latest_date': datetime.date(2023, 5, 11), 'pb_wo_gw_effective': Decimal('0.6452'), 'row_num': 55, 'total_num': 1214, 'percentage': 4.45, 'previous_year_num': '5'}], 'pe_ttm': [{'index_code': '399997', 'index_name': '中证白酒', 'latest_date': datetime.date(2023, 5, 11), 'pe_ttm_effective': Decimal('30.6833'), 'row_num': 26, 'total_num': 729, 'percentage': 3.43, 'previous_year_num': '3'}, {'index_code': '399997', 'index_name': '中证白酒', 'latest_date': datetime.date(2023, 5, 11), 'pe_ttm_nonrecurring_effective': Decimal('31.7515'), 'row_num': 28, 'total_num': 729, 'percentage': 3.71, 'previous_year_num': '3'}, {'index_code': '399997', 'index_name': '中证白酒', 'latest_date': datetime.date(2023, 5, 11), 'pe_ttm_effective': Decimal('30.6833'), 'row_num': 215, 'total_num': 1214, 'percentage': 17.64, 'previous_year_num': '5'}, {'index_code': '399997', 'index_name': '中证白酒', 'latest_date': datetime.date(2023, 5, 11), 'pe_ttm_nonrecurring_effective': Decimal('31.7515'), 'row_num': 313, 'total_num': 1214, 'percentage': 25.72, 'previous_year_num': '5'}], 'equity_bond_yield': [None, None]}
    """
    def generate_historical_percentage_estimation_info(self):
        #估值信息字典
        valuation_method_result_dict = dict()

        # 获取标的池中跟踪关注指数的指数代码，估值方式
        for index_unit in self.tracking_indexes_valuation_method_and_trigger_dict:
            index_code = index_unit["index_code"]
            valuation_method = index_unit["valuation_method"]
            # 如果该指数方式还未存入估值信息字典
            if valuation_method not in valuation_method_result_dict:
                valuation_method_result_dict[valuation_method] = []
            # 获取评估的时间长度
            for year in self._PREVIOUS_YEARS_LIST:
                # 获取当前指数估值在过去X年的百分比信息
                index_estimation_info = data_miner_common_index_operator.DataMinerCommonIndexOperator().get_index_latest_estimation_percentile_in_history(index_code, valuation_method, year)
                # 汇总在估值信息字典
                valuation_method_result_dict[valuation_method].append(index_estimation_info)
                # 如果遇到滚动市盈率，就把扣非滚动市盈率也算一遍
                if(valuation_method=="pe_ttm"):
                    valuation_method_extra = "pe_ttm_nonrecurring"
                    index_estimation_info = data_miner_common_index_operator.DataMinerCommonIndexOperator().get_index_latest_estimation_percentile_in_history(
                        index_code, valuation_method_extra, year)
                    valuation_method_result_dict[valuation_method].append(index_estimation_info)
                # 如果遇到市净率，就把扣非滚动市净率也算一遍
                elif (valuation_method=="pb"):
                    valuation_method_extra = "pb_wo_gw"
                    index_estimation_info = data_miner_common_index_operator.DataMinerCommonIndexOperator().get_index_latest_estimation_percentile_in_history(
                        index_code, valuation_method_extra, year)
                    valuation_method_result_dict[valuation_method].append(index_estimation_info)
        return valuation_method_result_dict

    """
    根据 生成所有指数在过去X年估值信息字典，生成提示信息
    """
    def generate_msg(self):
        # 将会生成的信息
        msg = ""
        # 生成所有指数在过去X年估值信息字典
        valuation_method_result_dict = self.generate_historical_percentage_estimation_info()
        index_code_last = None
        for valuation_method in valuation_method_result_dict:
            for unit in valuation_method_result_dict[valuation_method]:
                # 如果信息不为空
                if unit != None:
                    # 获取指数代码
                    index_code = unit["index_code"]
                    # 获取指数名称
                    index_name = unit["index_name"]
                    # 获取最新日期
                    latest_date  = unit["latest_date"]
                    # 获取处于历史百分比
                    percentage = unit["percentage"]
                    # 与过去X年的数据对比
                    previous_years = unit["previous_year_num"]
                    valuation_method = None
                    estimation_value = None
                    # 当前是哪一种估值方式
                    if("pe_ttm_effective" in unit):
                        valuation_method = "滚动市盈率"
                        estimation_value = unit['pe_ttm_effective']
                    elif ("pe_ttm_nonrecurring_effective" in unit):
                        valuation_method = "扣非滚动市盈率"
                        estimation_value = unit["pe_ttm_nonrecurring_effective"]
                    elif ("pb_effective" in unit):
                        valuation_method = "市净率"
                        estimation_value = unit["pb_effective"]
                    elif ("pb_wo_gw_effective" in unit):
                        valuation_method = "扣非市净率"
                        estimation_value = unit["pb_wo_gw_effective"]
                    elif (unit["ps_ttm_effective"] != None):
                        valuation_method = "滚动市销率"
                        estimation_value = unit["ps_ttm_effective"]
                    elif (unit["pcf_ttm_effective"] != None):
                        valuation_method = "滚动市现率"
                        estimation_value = unit["pcf_ttm_effective"]
                    elif (unit["dividend_yield_effective"] != None):
                        valuation_method = "股息率"
                        estimation_value = unit["dividend_yield_effective"]
                    # 如果当前指数代码与上一个不一致，说明已执行到新的指数
                    if(index_code_last != index_code):
                        # 新起一行
                        msg += "\n"+ str(latest_date) + " "+ index_name + "("+ index_code+")"+ "\n"
                    # 记录上一行的指数代码
                    index_code_last = index_code
                    # 记录估值信息
                    msg += valuation_method + ": " + str(decimal.Decimal(estimation_value)) + " 处于近" + previous_years + "年 "+ str(percentage) + "%" + "\n"
        # 日志记录，报告信息
        custom_logger.CustomLogger().log_writter(msg, 'info')

        # 日志记录
        log_msg = '标的指数盘后估值报告生成完毕'
        custom_logger.CustomLogger().log_writter(log_msg, 'info')
        return msg

if __name__ == '__main__':
    time_start = time.time()
    go = FundStrategyAfterTradingEstimationReport()
    result = go.generate_msg()
    #result = go.generate_historical_percentage_estimation_info()
    print(result)