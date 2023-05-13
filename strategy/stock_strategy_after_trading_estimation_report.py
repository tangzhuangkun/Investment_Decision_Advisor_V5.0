#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import decimal
import time
import sys

sys.path.append("..")
import log.custom_logger as custom_logger
import data_miner.data_miner_common_target_stock_operator as data_miner_common_target_stock_operator
import data_miner.data_miner_common_stock_operator as data_miner_common_stock_operator

"""
跟踪标的池中股票标的在盘后的估值情况，并生成报告
"""


class StockStrategyAfterTradingEstimationReport:

    def __init__(self):
        # 获取标的池中跟踪关注股票及对应的估值方式和触发条件(估值，低于等于历史百分位)
        # 如，[{'stock_code': '000002', 'stock_name': '万科A', 'stock_code_with_init': 'sz000002', 'stock_code_with_market_code': '000002.XSHE', 'valuation_method': 'pb', 'trigger_value': Decimal('0.95'), 'trigger_percent': Decimal('0.50')},
        # {'stock_code': '600048', 'stock_name': '保利发展', 'stock_code_with_init': 'sh600048', 'stock_code_with_market_code': '600048.XSHG', 'valuation_method': 'pb', 'trigger_value': Decimal('0.89'), 'trigger_percent': Decimal('10.00')},
        # {'stock_code': '000002', 'stock_name': '万科A', 'stock_code_with_init': 'sz000002', 'stock_code_with_market_code': '000002.XSHE', 'valuation_method': 'pe_ttm', 'trigger_value': Decimal('6.00'), 'trigger_percent': Decimal('5.00')}]
        self.tracking_stocks_valuation_method_and_trigger_dict = data_miner_common_target_stock_operator.DataMinerCommonTargetStockOperator().get_stocks_valuation_method_and_trigger()
        # 需要关注的过去x年的数据
        self._PREVIOUS_YEARS_LIST = [3, 4, 5, 7, 10]

    """
    生成所有标的股票在过去X年估值信息字典
    return： 例如  
    
    {'pb': [
	{'stock_code': '000002', 'stock_name': '万科A', 'date': datetime.date(2023, 5, 12), 'pb': Decimal('0.7303557081564865000000'), 'row_num': 12, 'total_record': 728, 'percentage': 1.51, 'previous_year_num': '3'}, 
	{'stock_code': '000002', 'stock_name': '万科A', 'date': datetime.date(2023, 5, 12), 'pb_wo_gw': Decimal('0.7467493793711947000000'), 'row_num': 12, 'total_record': 728, 'percentage': 1.51, 'previous_year_num': '3'}, 
	{'stock_code': '000002', 'stock_name': '万科A', 'date': datetime.date(2023, 5, 12), 'pb': Decimal('0.7303557081564865000000'), 'row_num': 12, 'total_record': 1215, 'percentage': 0.91, 'previous_year_num': '5'}, 
	{'stock_code': '000002', 'stock_name': '万科A', 'date': datetime.date(2023, 5, 12), 'pb_wo_gw': Decimal('0.7467493793711947000000'), 'row_num': 12, 'total_record': 1215, 'percentage': 0.91, 'previous_year_num': '5'}], 
	
'pe_ttm': [
	{'stock_code': '00700', 'stock_name': '腾讯控股', 'date': datetime.date(2023, 5, 12), 'pe_ttm': Decimal('14.8706095118916060000000'), 'row_num': 186, 'total_record': 739, 'percentage': 25.07, 'previous_year_num': '3'}, 
	{'stock_code': '00700', 'stock_name': '腾讯控股', 'date': datetime.date(2023, 5, 12), 'pe_ttm_nonrecurring': Decimal('0E-16'), 'row_num': 739, 'total_record': 739, 'percentage': 0.0, 'previous_year_num': '3'}, 
	{'stock_code': '00700', 'stock_name': '腾讯控股', 'date': datetime.date(2023, 5, 12), 'pe_ttm': Decimal('14.8706095118916060000000'), 'row_num': 186, 'total_record': 1232, 'percentage': 15.03, 'previous_year_num': '5'}, 
	{'stock_code': '00700', 'stock_name': '腾讯控股', 'date': datetime.date(2023, 5, 12), 'pe_ttm_nonrecurring': Decimal('0E-16'), 'row_num': 1232, 'total_record': 1232, 'percentage': 0.0, 'previous_year_num': '5'}, 
	
	{'stock_code': '09633', 'stock_name': '农夫山泉', 'date': datetime.date(2023, 5, 12), 'pe_ttm': Decimal('48.6554540388196800000000'), 'row_num': 9, 'total_record': 658, 'percentage': 1.22, 'previous_year_num': '3'}, 
	{'stock_code': '09633', 'stock_name': '农夫山泉', 'date': datetime.date(2023, 5, 12), 'pe_ttm_nonrecurring': Decimal('0E-16'), 'row_num': 658, 'total_record': 658, 'percentage': 0.0, 'previous_year_num': '3'}, 
	{'stock_code': '09633', 'stock_name': '农夫山泉', 'date': datetime.date(2023, 5, 12), 'pe_ttm': Decimal('48.6554540388196800000000'), 'row_num': 9, 'total_record': 658, 'percentage': 1.22, 'previous_year_num': '5'}, 
	{'stock_code': '09633', 'stock_name': '农夫山泉', 'date': datetime.date(2023, 5, 12), 'pe_ttm_nonrecurring': Decimal('0E-16'), 'row_num': 658, 'total_record': 658, 'percentage': 0.0, 'previous_year_num': '5'}, 
	
	{'stock_code': '600436', 'stock_name': '片仔癀', 'date': datetime.date(2023, 5, 12), 'pe_ttm': Decimal('71.4459811227165900000000'), 'row_num': 198, 'total_record': 728, 'percentage': 27.1, 'previous_year_num': '3'}, 
	{'stock_code': '600436', 'stock_name': '片仔癀', 'date': datetime.date(2023, 5, 12), 'pe_ttm_nonrecurring': Decimal('70.9453036709479200'), 'row_num': 168, 'total_record': 728, 'percentage': 22.97, 'previous_year_num': '3'}, 
	{'stock_code': '600436', 'stock_name': '片仔癀', 'date': datetime.date(2023, 5, 12), 'pe_ttm': Decimal('71.4459811227165900000000'), 'row_num': 632, 'total_record': 1215, 'percentage': 51.98, 'previous_year_num': '5'}, 
	{'stock_code': '600436', 'stock_name': '片仔癀', 'date': datetime.date(2023, 5, 12), 'pe_ttm_nonrecurring': Decimal('70.9453036709479200'), 'row_num': 591, 'total_record': 1215, 'percentage': 48.6, 'previous_year_num': '5'}]}
    """
    def generate_historical_percentage_estimation_info(self):
        #估值信息字典
        valuation_method_result_dict = dict()

        # 获取标的池中跟踪关注股票的股票代码，估值方式
        for stock_unit in self.tracking_stocks_valuation_method_and_trigger_dict:
            stock_code = stock_unit["stock_code"]
            valuation_method = stock_unit["valuation_method"]
            # 如果该股票方式还未存入估值信息字典
            if valuation_method not in valuation_method_result_dict:
                valuation_method_result_dict[valuation_method] = []
            # 获取评估的时间长度
            for year in self._PREVIOUS_YEARS_LIST:
                # 获取当前股票估值在过去X年的百分比信息
                stock_estimation_info = data_miner_common_stock_operator.DataMinerCommonStockOperator().get_stock_latest_estimation_percentile_in_history(stock_code, valuation_method, year)
                # 汇总在估值信息字典
                valuation_method_result_dict[valuation_method].append(stock_estimation_info)
                # 如果遇到滚动市盈率，就把扣非滚动市盈率也算一遍
                if(valuation_method=="pe_ttm"):
                    valuation_method_extra = "pe_ttm_nonrecurring"
                    stock_estimation_info = data_miner_common_stock_operator.DataMinerCommonStockOperator().get_stock_latest_estimation_percentile_in_history(
                        stock_code, valuation_method_extra, year)
                    valuation_method_result_dict[valuation_method].append(stock_estimation_info)
                # 如果遇到市净率，就把扣非滚动市净率也算一遍
                elif (valuation_method=="pb"):
                    valuation_method_extra = "pb_wo_gw"
                    stock_estimation_info = data_miner_common_stock_operator.DataMinerCommonStockOperator().get_stock_latest_estimation_percentile_in_history(
                        stock_code, valuation_method_extra, year)
                    valuation_method_result_dict[valuation_method].append(stock_estimation_info)
        return valuation_method_result_dict

    """
    根据 生成所有标的股票在过去X年估值信息字典，生成提示信息
    """
    def generate_msg(self):
        # 将会生成的信息
        msg = ""
        # 生成所有股票在过去X年估值信息字典
        valuation_method_result_dict = self.generate_historical_percentage_estimation_info()
        stock_code_last = None
        for valuation_method in valuation_method_result_dict:
            for unit in valuation_method_result_dict[valuation_method]:
                # 如果信息不为空
                if unit != None:
                    # 获取股票代码
                    stock_code = unit["stock_code"]
                    # 获取股票名称
                    stock_name = unit["stock_name"]
                    # 获取最新日期
                    latest_date  = unit["latest_date"]
                    # 获取处于历史百分比
                    percentage = unit["percentage"]
                    # 与过去X年的数据对比
                    previous_years = unit["previous_year_num"]
                    valuation_method = None
                    estimation_value = None
                    # 当前是哪一种估值方式
                    if("pe_ttm" in unit):
                        valuation_method = "滚动市盈率"
                        estimation_value = unit['pe_ttm']
                    elif ("pe_ttm_nonrecurring" in unit):
                        valuation_method = "扣非滚动市盈率"
                        estimation_value = unit["pe_ttm_nonrecurring"]
                    elif ("pb" in unit):
                        valuation_method = "市净率"
                        estimation_value = unit["pb"]
                    elif ("pb_wo_gw" in unit):
                        valuation_method = "扣非市净率"
                        estimation_value = unit["pb_wo_gw"]
                    elif (unit["ps_ttm"] != None):
                        valuation_method = "滚动市销率"
                        estimation_value = unit["ps_ttm"]
                    elif (unit["pcf_ttm"] != None):
                        valuation_method = "滚动市现率"
                        estimation_value = unit["pcf_ttm"]
                    elif (unit["dividend_yield"] != None):
                        valuation_method = "股息率"
                        estimation_value = unit["dividend_yield"]
                    # 如果当前股票代码与上一个不一致，说明已执行到新的股票
                    if(stock_code_last != stock_code):
                        # 新起一行
                        msg += "\n"+ str(latest_date) + " "+ stock_name + "("+ stock_code+")"+ "\n"
                    # 记录上一行的股票代码
                    stock_code_last = stock_code
                    # 记录估值信息
                    msg += valuation_method + ": " + str(decimal.Decimal(estimation_value)) + " 处于近" + previous_years + "年 "+ str(percentage) + "%" + "\n"
        # 日志记录，报告信息
        custom_logger.CustomLogger().log_writter(msg, 'info')

        # 日志记录
        log_msg = '标的股票盘后估值报告生成完毕'
        custom_logger.CustomLogger().log_writter(log_msg, 'info')
        return msg



if __name__ == '__main__':
    time_start = time.time()
    go = StockStrategyAfterTradingEstimationReport()
    result = go.generate_msg()
    #result = go.generate_historical_percentage_estimation_info()
    print(result)