#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import sys
import time

sys.path.append("..")
import log.custom_logger as custom_logger
import db_mapper.aggregated_data.stock_bond_ratio_di_mapper as stock_bond_ratio_di_mapper
import db_mapper.financial_data.trading_days_mapper as trading_days_mapper

class StockBondStrategyEquityBondYield:
    # 择时策略，股债收益率
    # 沪深300指数市值加权估值PE/十年国债收益率
    # 用于判断股市收益率与无风险收益之间的比值
    # 频率：每个交易日，盘后

    def __init__(self):
        # 需要关注的过去x年的数据
        self._PREVIOUS_YEARS_LIST = [3, 4, 5, 8, 10]
        # 沪深300指数代码
        self.index_code = "000300"

    """
    生成关于股债收益率统计数据的通知信息
    ：return，
    2023-06-05 
    沪深300指数: -0.46% 
    市盈率: 11.8349 
    10年期国债收益率: 2.6926% 
    股债收益比:  3.1381 
    近3年历史排位： 89.67 % 
    近4年历史排位： 87.94 % 
    近5年历史排位： 90.27 % 
    近8年历史排位： 93.16 % 
    近10年历史排位： 94.28 % 
    """
    def generate_latest_notification_msg(self):

        # 各跟踪年份的股债收益率
        all_tracking_year_stock_bond_ratio_list = list()
        # 今天的日期
        today = time.strftime("%Y-%m-%d", time.localtime())
        # 最新的交易日期
        latest_trading_date = trading_days_mapper.TradingDaysMapper().get_the_lastest_trading_date(today)

        # {'index_code': '000300', 'index_name': '沪深300', 'p_day': datetime.date(2023, 6, 2),
        #     'index_change_rate': Decimal('0.01443705721498239800'), 'index_pe': Decimal('11.84731402813075200'),
        #     'stock_yield_rate': Decimal('0.084407'), '10y_bond_rate': Decimal('0.026951'),
        #     'ratio': Decimal('3.13186894734889243')}
        stock_bond_ratio_info = stock_bond_ratio_di_mapper.StockBondRatioDiMapper().get_a_specific_day_stock_bond_ratio_info(self.index_code , latest_trading_date)

        # 今天收盘的股债收益比
        latest_trading_date_ratio = stock_bond_ratio_info.get("ratio")
        # 指数市盈率
        index_pe = stock_bond_ratio_info.get("index_pe")
        # 10年国债收益率
        ten_years_bond_rate = stock_bond_ratio_info.get("10y_bond_rate")
        # 指数涨跌幅
        index_change_rate = stock_bond_ratio_info.get("index_change_rate")

        for year_num in self._PREVIOUS_YEARS_LIST:
            # {'index_code': '000300', 'index_name': '沪深300', 'p_day': datetime.date(2023, 6, 2), 'index_change_rate': Decimal('0.01443705721498239800'), 'index_pe': Decimal('11.84731402813075200'), 'stock_yield_rate': Decimal('0.084407'), '10y_bond_rate': Decimal('0.026951'), 'ratio': Decimal('3.13186894734889243'), 'percent': 0.8928571428571429}
            stock_bond_ratio_percent_info = stock_bond_ratio_di_mapper.StockBondRatioDiMapper().get_a_specific_date_stock_bond_ratio_percentile_in_history(self.index_code, latest_trading_date, year_num)
            all_tracking_year_stock_bond_ratio_list.append(stock_bond_ratio_percent_info.get("percent")*100)

        # 生成的消息
        msg = ''
        msg += latest_trading_date + ' \n'
        msg += '沪深300: ' + str(round(index_change_rate*100, 2))+'%' +  ' \n'
        msg += '市盈率: ' + str(round(index_pe, 4)) +  ' \n'
        msg += '10年期国债收益率: ' + str(round(ten_years_bond_rate*100, 4)) +'%' +  ' \n'
        msg += '股债收益比:  ' + str(round(latest_trading_date_ratio,4)) + ' \n'
        for year_index in range(len(self._PREVIOUS_YEARS_LIST)):
            msg += '近'+ str(self._PREVIOUS_YEARS_LIST[year_index]) +'年历史排位： ' + str(round(all_tracking_year_stock_bond_ratio_list[year_index],2)) + ' %' + ' \n'

        # 日志记录，报告信息
        custom_logger.CustomLogger().log_writter(msg, 'info')
        # 日志记录
        log_msg = '盘后股债收益率信息生成完毕'
        custom_logger.CustomLogger().log_writter(log_msg, 'info')
        return msg

    def main(self):
        return self.generate_latest_notification_msg()

if __name__ == '__main__':
    time_start = time.time()
    go = StockBondStrategyEquityBondYield()
    result = go.main()
    print(result)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))