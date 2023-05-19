#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import decimal
import time
import sys
import collections

sys.path.append("..")
import log.custom_logger as custom_logger
import data_miner.data_miner_common_target_index_operator as data_miner_common_target_index_operator
import data_miner.data_miner_common_index_operator as data_miner_common_index_operator
import data_collector.get_target_real_time_indicator_from_interfaces as get_target_real_time_indicator_from_interfaces

"""
跟踪标的池中指数基金标的在盘后的估值情况，并生成报告
"""


class FundStrategyAfterTradingEstimationReport:

    def __init__(self):
        # 获取标的池中跟踪关注指数的指数代码，中文名称, 地点缩写+指数代码，指数代码+证券市场代码，估值方式
        #  如 [{'index_code': '399997', 'index_name': '中证白酒指数', 'index_code_with_init': 'sz399997', 'index_code_with_market_code': '399997.XSHE', 'valuation_method': 'pe_ttm'},，，，]
        self.tracking_indexes_valuation_method_and_trigger_dict = data_miner_common_target_index_operator.DataMinerCommonTargetIndexOperator().get_index_valuation_method()
        # 需要关注的过去x年的数据
        self._PREVIOUS_YEARS_LIST = [3,4,5,8,10]

    """
    生成所有标的指数在过去X年估值信息字典
    return： 例如  
    {'pe_ttm': 
    [{'index_code': '000932', 'index_name': '中证800消费', 'latest_date': datetime.date(2023, 5, 12), 'pe_ttm_effective': Decimal('31.5048'), 'row_num': 2, 'total_num': 726, 'percentage': 0.14, 'previous_year_num': '3', 'index_code_with_init': 'sh000932'}, 
    {'index_code': '000932', 'index_name': '中证800消费', 'latest_date': datetime.date(2023, 5, 12), 'pe_ttm_nonrecurring_effective': Decimal('38.5319'), 'row_num': 19, 'total_num': 726, 'percentage': 2.48, 'previous_year_num': '3', 'index_code_with_init': 'sh000932'}, 
    {'index_code': '000932', 'index_name': '中证800消费', 'latest_date': datetime.date(2023, 5, 12), 'pe_ttm_effective': Decimal('31.5048'), 'row_num': 2, 'total_num': 968, 'percentage': 0.1, 'previous_year_num': '4', 'index_code_with_init': 'sh000932'}, 
    {'index_code': '000932', 'index_name': '中证800消费', 'latest_date': datetime.date(2023, 5, 12), 'pe_ttm_nonrecurring_effective': Decimal('38.5319'), 'row_num': 53, 'total_num': 968, 'percentage': 5.38, 'previous_year_num': '4', 'index_code_with_init': 'sh000932'}, 
    {'index_code': '399997', 'index_name': '中证白酒', 'latest_date': datetime.date(2023, 5, 12), 'pe_ttm_effective': Decimal('30.2578'), 'row_num': 22, 'total_num': 726, 'percentage': 2.9, 'previous_year_num': '3', 'index_code_with_init': 'sz399997'}, 
    {'index_code': '399997', 'index_name': '中证白酒', 'latest_date': datetime.date(2023, 5, 12), 'pe_ttm_nonrecurring_effective': Decimal('31.3089'), 'row_num': 23, 'total_num': 726, 'percentage': 3.03, 'previous_year_num': '3', 'index_code_with_init': 'sz399997'}, 
    {'index_code': '399997', 'index_name': '中证白酒', 'latest_date': datetime.date(2023, 5, 12), 'pe_ttm_effective': Decimal('30.2578'), 'row_num': 86, 'total_num': 968, 'percentage': 8.79, 'previous_year_num': '4', 'index_code_with_init': 'sz399997'}, 
    {'index_code': '399997', 'index_name': '中证白酒', 'latest_date': datetime.date(2023, 5, 12), 'pe_ttm_nonrecurring_effective': Decimal('31.3089'), 'row_num': 112, 'total_num': 968, 'percentage': 11.48, 'previous_year_num': '4', 'index_code_with_init': 'sz399997'}], 
    
    'pb': 
    [{'index_code': '399986', 'index_name': '中证银行', 'latest_date': datetime.date(2023, 5, 12), 'pb_effective': Decimal('0.6362'), 'row_num': 42, 'total_num': 726, 'percentage': 5.66, 'previous_year_num': '3', 'index_code_with_init': 'sz399986'}, 
    {'index_code': '399986', 'index_name': '中证银行', 'latest_date': datetime.date(2023, 5, 12), 'pb_wo_gw_effective': Decimal('0.6389'), 'row_num': 41, 'total_num': 726, 'percentage': 5.52, 'previous_year_num': '3', 'index_code_with_init': 'sz399986'}, 
    {'index_code': '399986', 'index_name': '中证银行', 'latest_date': datetime.date(2023, 5, 12), 'pb_effective': Decimal('0.6362'), 'row_num': 42, 'total_num': 968, 'percentage': 4.24, 'previous_year_num': '4', 'index_code_with_init': 'sz399986'}, 
    {'index_code': '399986', 'index_name': '中证银行', 'latest_date': datetime.date(2023, 5, 12), 'pb_wo_gw_effective': Decimal('0.6389'), 'row_num': 41, 'total_num': 968, 'percentage': 4.14, 'previous_year_num': '4', 'index_code_with_init': 'sz399986'}]
    }
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
                # 添加上市地+指数代码， 如，sz399997
                index_estimation_info["index_code_with_init"] = index_unit["index_code_with_init"]
                # 汇总在估值信息字典
                valuation_method_result_dict[valuation_method].append(index_estimation_info)
                # 如果遇到滚动市盈率，就把扣非滚动市盈率也算一遍
                if(valuation_method=="pe_ttm"):
                    valuation_method_extra = "pe_ttm_nonrecurring"
                    index_estimation_info = data_miner_common_index_operator.DataMinerCommonIndexOperator().get_index_latest_estimation_percentile_in_history(
                        index_code, valuation_method_extra, year)
                    # 添加上市地+指数代码， 如，sz399997
                    index_estimation_info["index_code_with_init"] = index_unit["index_code_with_init"]
                    valuation_method_result_dict[valuation_method].append(index_estimation_info)
                # 如果遇到市净率，就把扣非滚动市净率也算一遍
                elif (valuation_method=="pb"):
                    valuation_method_extra = "pb_wo_gw"
                    index_estimation_info = data_miner_common_index_operator.DataMinerCommonIndexOperator().get_index_latest_estimation_percentile_in_history(
                        index_code, valuation_method_extra, year)
                    # 添加上市地+指数代码， 如，sz399997
                    index_estimation_info["index_code_with_init"] = index_unit["index_code_with_init"]
                    valuation_method_result_dict[valuation_method].append(index_estimation_info)

        # 获取评估的时间长度
        for year in self._PREVIOUS_YEARS_LIST:
            # 获取沪深300指数滚动市盈率估值在过去X年的百分比信息
            index_estimation_info = data_miner_common_index_operator.DataMinerCommonIndexOperator().get_hz_three_hundred_index_latest_estimation_percentile_in_history('000300', "pe_ttm", year)
            valuation_method_result_dict["pe_ttm"].append(index_estimation_info)
        return valuation_method_result_dict

    """
    根据 生成所有指数在过去X年估值信息字典，生成提示信息
    
    2023-05-12 中证白酒(399997) -1.05%
    滚动市盈率: 30.2578 处于近3年 2.9%
    扣非滚动市盈率: 31.3089 处于近3年 3.03%
    滚动市盈率: 30.2578 处于近4年 8.79%
    扣非滚动市盈率: 31.3089 处于近4年 11.48%
    滚动市盈率: 30.2578 处于近5年 15.62%
    扣非滚动市盈率: 31.3089 处于近5年 22.98%
    滚动市盈率: 30.2578 处于近8年 10.04%
    扣非滚动市盈率: 31.3089 处于近8年 14.62%
    滚动市盈率: 30.2578 处于近10年 21.71%
    扣非滚动市盈率: 31.3089 处于近10年 24.88%
    
    2023-05-12 中证银行(399986) -0.75%
    市净率: 0.6362 处于近3年 5.66%
    扣非市净率: 0.6389 处于近3年 5.52%
    市净率: 0.6362 处于近4年 4.24%
    扣非市净率: 0.6389 处于近4年 4.14%
    市净率: 0.6362 处于近5年 3.39%
    扣非市净率: 0.6389 处于近5年 3.31%
    市净率: 0.6362 处于近8年 2.11%
    扣非市净率: 0.6389 处于近8年 2.06%
    市净率: 0.6362 处于近10年 1.69%
    扣非市净率: 0.6389 处于近10年 1.65%
    """
    def generate_msg(self):
        # 将会生成的信息
        msg = ""
        # 生成所有指数在过去X年估值信息字典
        valuation_method_result_dict = self.generate_historical_percentage_estimation_info()
        # 上一个指数代码
        index_code_last = None
        # 估值中文名称
        valuation_method_name = None
        # 估值的值
        estimation_value = None
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
                    # 上市地+指数代码， 如，sz399997
                    index_code_with_init = unit["index_code_with_init"]
                    change = get_target_real_time_indicator_from_interfaces.GetTargetRealTimeIndicatorFromInterfaces().get_single_target_real_time_indicator(
                        index_code_with_init, "change")
                    # 当前是哪一种估值方式
                    if ("pe_ttm" in unit):
                        valuation_method_name = "滚动市盈率"
                        estimation_value = unit["pe_ttm"]
                    elif("pe_ttm_effective" in unit):
                        valuation_method_name = "滚动市盈率"
                        estimation_value = unit['pe_ttm_effective']
                    elif ("pe_ttm_nonrecurring_effective" in unit):
                        valuation_method_name = "扣非滚动市盈率"
                        estimation_value = unit["pe_ttm_nonrecurring_effective"]
                    elif ("pb_effective" in unit):
                        valuation_method_name = "市净率"
                        estimation_value = unit["pb_effective"]
                    elif ("pb_wo_gw_effective" in unit):
                        valuation_method_name = "扣非市净率"
                        estimation_value = unit["pb_wo_gw_effective"]
                    elif ("ps_ttm_effective" in unit):
                        valuation_method_name = "滚动市销率"
                        estimation_value = unit["ps_ttm_effective"]
                    elif ("pcf_ttm_effective" in unit):
                        valuation_method_name = "滚动市现率"
                        estimation_value = unit["pcf_ttm_effective"]
                    elif ("dividend_yield_effective" in unit):
                        valuation_method_name = "股息率"
                        estimation_value = unit["dividend_yield_effective"]
                    # 如果当前指数代码与上一个不一致，说明已执行到新的指数
                    if(index_code_last != index_code):
                        # 新起一行 , 生成信息表格的表头，日期，指数名称，指数代码, 当日涨跌幅
                        msg += "\n"+ str(latest_date) + " "+ index_name + "("+ index_code+") "+ change + "%" + "\n"
                    # 记录上一行的指数代码
                    index_code_last = index_code
                    # 记录估值信息
                    msg += valuation_method_name + ": " + str(decimal.Decimal(estimation_value)) + " 处于近" + previous_years + "年 "+ str(percentage) + "%" + "\n"
        # 日志记录，报告信息
        custom_logger.CustomLogger().log_writter(msg, 'info')

        # 日志记录
        log_msg = '标的指数盘后估值报告生成完毕'
        custom_logger.CustomLogger().log_writter(log_msg, 'info')
        return msg

    """
     根据 生成所有指数在过去X年估值信息字典，生成报表形式的提示信息
     如
     2023-05-12 中证白酒(399997) -1.05%
    估值方式 | 值 | 3年 | 4年 | 5年 | 8年 | 10年 
    滚动市盈率 | 30.2578 | 2.9% | 8.79% | 15.62% | 10.04% | 21.71% | 
    扣非滚动市盈率 | 31.3089 | 3.03% | 11.48% | 22.98% | 14.62% | 24.88% | 
    
    2023-05-12 中证银行(399986) -0.75%
    估值方式 | 值 | 3年 | 4年 | 5年 | 8年 | 10年 
    市净率 | 0.6362 | 5.66% | 4.24% | 3.39% | 2.11% | 1.69% | 
    扣非市净率 | 0.6389 | 5.52% | 4.14% | 3.31% | 2.06% | 1.65% | 
     
     """

    def generate_form_msg(self):
        # 将会生成的信息
        msg = ""
        # 生成所有指数在过去X年估值信息字典
        valuation_method_result_dict = self.generate_historical_percentage_estimation_info()
        # 用于存储上一个处理的指数代码
        index_code_last = ""
        # 有序字典
        msg_valuation_row_dict = collections.OrderedDict()
        # 估值中文名称
        valuation_method_name = None
        # 估值的值
        estimation_value = None
        # percentage = None
        for valuation_method in valuation_method_result_dict:
            for unit in valuation_method_result_dict[valuation_method]:
                # 如果信息不为空
                if unit != None:
                    # 获取指数代码
                    index_code = unit["index_code"]
                    # 获取指数名称
                    index_name = unit["index_name"]
                    # 获取最新日期
                    latest_date = unit["latest_date"]
                    # 获取处于历史百分比
                    percentage = unit["percentage"]
                    # 与过去X年的数据对比
                    previous_years = unit["previous_year_num"]
                    # 上市地+指数代码， 如，sz399997
                    index_code_with_init = unit["index_code_with_init"]
                    change = get_target_real_time_indicator_from_interfaces.GetTargetRealTimeIndicatorFromInterfaces().get_single_target_real_time_indicator(
                        index_code_with_init, "change")
                    # 当前是哪一种估值方式
                    if ("pe_ttm" in unit):
                        valuation_method_name = "滚动市盈率"
                        estimation_value = unit["pe_ttm"]
                    elif ("pe_ttm_effective" in unit):
                        valuation_method_name = "滚动市盈率"
                        estimation_value = unit['pe_ttm_effective']
                    elif ("pe_ttm_nonrecurring_effective" in unit):
                        valuation_method_name = "扣非滚动市盈率"
                        estimation_value = unit["pe_ttm_nonrecurring_effective"]
                    elif ("pb_effective" in unit):
                        valuation_method_name = "市净率"
                        estimation_value = unit["pb_effective"]
                    elif ("pb_wo_gw_effective" in unit):
                        valuation_method_name = "扣非市净率"
                        estimation_value = unit["pb_wo_gw_effective"]
                    elif ("ps_ttm_effective" in unit):
                        valuation_method_name = "滚动市销率"
                        estimation_value = unit["ps_ttm_effective"]
                    elif ("pcf_ttm_effective" in unit):
                        valuation_method_name = "滚动市现率"
                        estimation_value = unit["pcf_ttm_effective"]
                    elif ("dividend_yield_effective" in unit):
                        valuation_method_name = "股息率"
                        estimation_value = unit["dividend_yield_effective"]
                    # 如果当前指数代码与上一个不一致，说明已执行到新的指数
                    if (index_code_last != index_code):
                        # 如果指数过去x年的估值信息不为空
                        # 使用有序字典进行储存
                        # 如 OrderedDict([('滚动市盈率', ['滚动市盈率', '14.8706', 25.1, 18.76, 15.04, 10.74, 7.93]), ('扣非滚动市盈率', ['扣非滚动市盈率', '0.0000', 0.0, 0.0, 0.0, 0.0, 0.0])])
                        # 参考 self._PREVIOUS_YEARS_LIST = [3, 4, 5, 7, 10]
                        # 字典中的value部分，即list部分，内容代表，[估值中文名，估值的值，过去3年的历史百分位，过去4年的历史百分位，过去5年的历史百分位，过去7年的历史百分位，过去10年的历史百分位]
                        if (msg_valuation_row_dict):
                            # 将字典中储存的信息转为文字提示信息
                            for index_valuation_name in msg_valuation_row_dict:
                                for index_valuation_years_info in msg_valuation_row_dict[index_valuation_name]:
                                    msg += str(index_valuation_years_info) + " | "
                                msg += "\n"
                        # 清空之前指数的记录
                        msg_date_index_code_name_row = ""
                        msg_year_row = ""
                        msg_valuation_row_dict.clear()

                        # 新起
                        # 单个指数，生成信息表格的表头，日期，指数名称，指数代码, 当日涨跌幅
                        msg_date_index_code_name_row += "\n" + str(
                            latest_date) + " " + index_name + "(" + index_code + ") "+ change + "%"  + "\n"
                        # 过去X年的展示列表
                        msg_year_row += "估值方式 | " + "值 "
                        for year_num in self._PREVIOUS_YEARS_LIST:
                            msg_year_row += "| " + str(year_num) + "年 "
                        msg_year_row += "\n"
                        # 合并
                        msg += msg_date_index_code_name_row + msg_year_row
                        # 在有序字典中，存入 [估值中文名，估值的值]
                        msg_valuation_row_dict[valuation_method_name] = [valuation_method_name,
                                                                         str(decimal.Decimal(estimation_value))]
                    # 记录上一行的指数代码
                    index_code_last = index_code
                    # 如果估值中文名已在字典中，储存对应的过去x年历史百分位
                    if (valuation_method_name in msg_valuation_row_dict):
                        msg_valuation_row_dict[valuation_method_name].append(str(percentage) + "%")
                    # 如果估值中文名不在字典中，[估值中文名，估值的值，对应的过去x年历史百分位]
                    else:
                        msg_valuation_row_dict[valuation_method_name] = [valuation_method_name,
                                                                         str(decimal.Decimal(estimation_value)),
                                                                         str(percentage) + "%"]
        # 所有信息已处理完毕，将最后字典中储存的信息转为提示文字
        if (msg_valuation_row_dict):
            for index_valuation_name in msg_valuation_row_dict:
                for index_valuation_years_info in msg_valuation_row_dict[index_valuation_name]:
                    msg += str(index_valuation_years_info) + " | "
                msg += "\n"

        # 日志记录，报告信息
        custom_logger.CustomLogger().log_writter(msg, 'info')

        # 日志记录
        log_msg = '标的指数盘后估值报表生成完毕'
        custom_logger.CustomLogger().log_writter(log_msg, 'info')
        return msg

if __name__ == '__main__':
    time_start = time.time()
    go = FundStrategyAfterTradingEstimationReport()
    result = go.generate_msg()
    #result = go.generate_form_msg()
    #result = go.generate_historical_percentage_estimation_info()
    time_end = time.time()
    print(result)
    print('time:')
    print(time_end - time_start)