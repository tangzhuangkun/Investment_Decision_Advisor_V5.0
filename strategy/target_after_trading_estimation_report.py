#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import time

import sys

sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger
import data_miner.data_miner_common_target_index_operator as data_miner_common_target_index_operator
import data_miner.data_miner_common_target_stock_operator as data_miner_common_target_stock_operator

"""
跟踪标的池中标的在盘后的估值情况，并生成报告
"""


class TargetAfterTradingEstimationReport:

    def __init__(self):
        # 获取标的池中跟踪关注股票及对应的估值方式和触发条件(估值，低于等于历史百分位)
        # 如，[{'stock_code': '000002', 'stock_name': '万科A', 'stock_code_with_init': 'sz000002', 'stock_code_with_market_code': '000002.XSHE', 'valuation_method': 'pb', 'trigger_value': Decimal('0.95'), 'trigger_percent': Decimal('0.50')}, {'stock_code': '600048', 'stock_name': '保利发展', 'stock_code_with_init': 'sh600048', 'stock_code_with_market_code': '600048.XSHG', 'valuation_method': 'pb', 'trigger_value': Decimal('0.89'), 'trigger_percent': Decimal('10.00')}, {'stock_code': '000002', 'stock_name': '万科A', 'stock_code_with_init': 'sz000002', 'stock_code_with_market_code': '000002.XSHE', 'valuation_method': 'pe_ttm', 'trigger_value': Decimal('6.00'), 'trigger_percent': Decimal('5.00')}]
        tracking_stocks_valuation_method_and_trigger_dict = data_miner_common_target_stock_operator.DataMinerCommonTargetStockOperator().get_stocks_valuation_method_and_trigger()

        # 获取标的池中跟踪关注指数的指数代码，中文名称, 地点缩写+指数代码，指数代码+证券市场代码，估值方式
        #  如 [{'index_code': '399997', 'index_name': '中证白酒指数', 'index_code_with_init': 'sz399997', 'index_code_with_market_code': '399997.XSHE', 'valuation_method': 'pe_ttm'},，，，]
        tracking_indexes_valuation_method_and_trigger_dict = data_miner_common_target_index_operator.DataMinerCommonTargetIndexOperator().get_index_valuation_method()




if __name__ == '__main__':
    time_start = time.time()
    go = TargetAfterTradingEstimationReport()
