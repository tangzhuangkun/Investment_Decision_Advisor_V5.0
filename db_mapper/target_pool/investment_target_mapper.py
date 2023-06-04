#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import time
import sys

sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger

"""
数据表，investment_target 的映射
以该表为主的数据操作，均在此完成
"""


class InvestmentTargetMapper:

    def __init__(self):
        pass

    """
    获取通过xx估值法 估值的标的代码及其对应名称
    :param target_type, 标的类型，index--指数，stock--股票， stock_bond--股债
    :param valuation_method 估值方式，pe_ttm--滚动市盈率, pe_ttm_nonrecurring--扣非滚动市盈率, pb--市净率, pb_wo_gw--扣非市净率, ps_ttm--滚动市销率, pcf_ttm--滚动市现率, dividend_yield--股息率，equity_bond_yield--股债收益率
    :param status, 当前处于激活还是停用状态，active--启用，inactive--停用，suspend--暂停
    :trade_direction, 交易方向，buy--买入，sell--卖出
    :return 
    如 [{'target_code': '000932', 'target_name': '中证800消费', 'target_code_with_init': 'sh000932', 'target_code_with_market_code': '000932.XSHE'}, 
        {'target_code': '399997', 'target_name': '中证白酒', 'target_code_with_init': 'sz399997', 'target_code_with_market_code': '399997.XSHE'}]
    如 [{'target_code': '000568', 'target_name': '泸州老窖', 'target_code_with_init': 'sz000568', 'target_code_with_market_code': '000568.XSHE'}, 
        {'target_code': '000858', 'target_name': '五粮液', 'target_code_with_init': 'sz000858', 'target_code_with_market_code': '000858.XSHE'}, ,,, ]
    如 [{'target_code': 'diy_000300_cn10yr', 'target_name': '股债收益率', 'target_code_with_init': 'shdiy_000300_cn10yr', 'target_code_with_market_code': 'diy_000300_cn10yr.XSHG'}]
    
    """

    def get_target_valuated_by_method(self, target_type, valuation_method, status, trade_direction):

        # 查询SQL
        selecting_sql = """select target_code, target_name, 
        concat(exchange_location,target_code) as target_code_with_init, 
        concat(target_code,'.',exchange_location_mic) as target_code_with_market_code 
        from target_pool.investment_target 
        where target_type = '%s' and status = '%s' and trade='%s' and valuation_method = '%s' """ \
                        % (target_type, status, trade_direction, valuation_method)

        # 查询
        selecting_result = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        return selecting_result


    """
    查询标的池中各标的估值方式
    :param target_type, 标的类型，index--指数，stock--股票， stock_bond--股债
    :param status, 当前处于激活还是停用状态，active--启用，inactive--停用，suspend--暂停
    :trade_direction, 交易方向，buy--买入，sell--卖出
    ：return，
    [{'target_code': '000932', 'target_name': '中证800消费', 'target_code_with_init': 'sh000932', 'target_code_with_market_code': '000932.XSHE', 'valuation_method': 'pe_ttm'}, 
    {'target_code': '399965', 'target_name': '中证800地产', 'target_code_with_init': 'sz399965', 'target_code_with_market_code': '399965.XSHE', 'valuation_method': 'pb'}, 
    {'target_code': '399986', 'target_name': '中证银行', 'target_code_with_init': 'sz399986', 'target_code_with_market_code': '399986.XSHE', 'valuation_method': 'pb'}, 
    {'target_code': '399997', 'target_name': '中证白酒', 'target_code_with_init': 'sz399997', 'target_code_with_market_code': '399997.XSHE', 'valuation_method': 'pe_ttm'}]
    """
    def get_target_valuation_method(self, target_type, status, trade_direction):

        # 查询SQL
        selecting_sql = """select target_code, target_name, 
        concat(exchange_location,target_code) as target_code_with_init, 
        concat(target_code,'.',exchange_location_mic) as target_code_with_market_code,
        valuation_method as  valuation_method
        from target_pool.investment_target 
        where target_type = '%s' and status = '%s' and trade='%s'""" \
                        % (target_type, status, trade_direction)

        # 查询
        selecting_result = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        return selecting_result


if __name__ == '__main__':
    time_start = time.time()
    go = InvestmentTargetMapper()
    #result = go.get_target_valuated_by_method("stock_bond", "equity_bond_yield", "active", "buy")
    result = go.get_target_valuation_method("index","active", "buy")
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)
