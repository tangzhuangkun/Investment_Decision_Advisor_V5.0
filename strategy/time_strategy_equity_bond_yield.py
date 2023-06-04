#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import sys
import time
import decimal

sys.path.append("..")
import database.db_operator as db_operator
import data_collector.collect_chn_gov_bonds_rates as collect_chn_gov_bonds_rates
import data_collector.collect_index_estimation_from_lxr as collect_index_estimation_from_lxr
import data_miner.calculate_stock_bond_ratio as calculate_stock_bond_ratio
import db_mapper.aggregated_data.stock_bond_ratio_di_mapper as stock_bond_ratio_di_mapper

class TimeStrategyEquityBondYield:
    # 择时策略，股债收益率
    # 沪深300指数市值加权估值PE/十年国债收益率
    # 用于判断股市收益率与无风险收益之间的比值
    # 频率：每个交易日，盘后

    def __init__(self):
        pass

    def prepare_index_estimation_bond_rate_and_cal_yield(self):
        # 准备数据，收集最新沪深300指数市值加权估值和国债利率,
        # 并计算当日收盘后的真实股债收益率
        # 盘后执行

        # TODO 拆分为独立的采集任务
        # 收集最新国债收益率
        collect_chn_gov_bonds_rates.CollectCHNGovBondsRates().main()
        # 收集最新沪深300指数市值加权估值
        collect_index_estimation_from_lxr.CollectIndexEstimationFromLXR().main()
        # 运行mysql脚本，计算股债收益率
        calculate_stock_bond_ratio.CalculateStockBondRatio().main()


    # TODO  替换为SQL 中执行
    def cal_equity_bond_yield_historical_rank(self, year_count, yield_ratio):
        # 估算当前实时股债收益比的与历史数据相比的排位
        # year_count： 年数，时间跨度，必须为整数，如 2
        # yield_ratio: 股债收益比, 如 0.0806524
        # return：年数, 股债收益比, 历史排位百分比, 如   (2, 2.8855, 87.81)
        selecting_sql = "select ratio from stock_bond_ratio_di " \
                        "where index_code = '000300' and trading_date > subdate(now(), interval '%s' year) " \
                        "order by ratio desc" % (year_count)

        # 查询历史上所有股债比
        all_historical_ratio_list = db_operator.DBOperator().select_all("aggregated_data", selecting_sql)
        # 历史上总共有多少数据量
        all_historical_ratio_count = len(all_historical_ratio_list)
        # 如果是极端值，大于历史上的最大值,100%情况
        if(yield_ratio>=all_historical_ratio_list[0]["ratio"]):
            return (year_count, round(float(yield_ratio),8),100)
        # 如果是极端值，小于历史上的最小值,0%情况
        elif(yield_ratio<all_historical_ratio_list[all_historical_ratio_count-1]["ratio"]):
            return (year_count, round(float(yield_ratio),8), 0)
        # 处于0%-100%之间
        else:
            for ratio_index in range(all_historical_ratio_count):
                if(yield_ratio>all_historical_ratio_list[ratio_index]["ratio"]):
                    return (year_count, round(float(yield_ratio),8), round(((1-ratio_index/all_historical_ratio_count)*100),2))


    def generate_today_notification_msg(self):
        # 每日收盘后，单纯生成统计数据的通知信息

        # 准备数据，收集最新沪深300指数市值加权估值和国债利率,
        # 并计算当日收盘后的真实股债收益率
        self.prepare_index_estimation_bond_rate_and_cal_yield()

        # TODO 替换为最新交易日
        # 当天的日期
        today = time.strftime("%Y-%m-%d", time.localtime())
        # {'index_code': '000300', 'index_name': '沪深300', 'p_day': datetime.date(2023, 6, 2),
        #     'index_change_rate': Decimal('0.01443705721498239800'), 'index_pe': Decimal('11.84731402813075200'),
        #     'stock_yield_rate': Decimal('0.084407'), '10y_bond_rate': Decimal('0.026951'),
        #     'ratio': Decimal('3.13186894734889243')}
        stock_bond_ratio_info = stock_bond_ratio_di_mapper.StockBondRatioDiMapper().get_a_specific_day_stock_bond_ratio_info("000300", today)
        # 今天收盘的股债收益比
        today_ratio = stock_bond_ratio_info.get("ratio")
        # 指数市盈率
        index_pe = stock_bond_ratio_info.get("index_pe")
        # 10年国债收益率
        ten_years_bond_rate = stock_bond_ratio_info.get("10y_bond_rate")

        # TODO 采用列表控制年份
        # 今天股债收益比在近3，5，8，10年的排位信息
        # 年数, 今天股债收益比, 历史排位百分比, 如   (2, 2.8855, 87.81)
        three_year_equity_bond_yield_info = self.cal_equity_bond_yield_historical_rank(3, today_ratio)
        five_year_equity_bond_yield_info = self.cal_equity_bond_yield_historical_rank(5, today_ratio)
        eight_year_equity_bond_yield_info = self.cal_equity_bond_yield_historical_rank(8,today_ratio)
        ten_year_equity_bond_yield_info = self.cal_equity_bond_yield_historical_rank(10,today_ratio)

        # TODO  生成信息中，加入指数涨跌幅
        # TODO  生成信息中，指数PE
        # TODO  生成信息中，债券收益率
        # 生成的消息
        msg = ''
        msg += today + ' \n'
        msg += '基于沪深300指数' + ' \n'
        msg += '股债收益比： ' + str(round(today_ratio,8)) + ' \n'
        msg += '近3年历史排位： ' + str(three_year_equity_bond_yield_info[2]) + ' %' + ' \n'
        msg += '近5年历史排位： ' + str(five_year_equity_bond_yield_info[2]) + ' %' + ' \n'
        msg += '近8年历史排位： ' + str(eight_year_equity_bond_yield_info[2]) + ' %' + ' \n'
        msg += '近10年历史排位： ' + str(ten_year_equity_bond_yield_info[2]) + ' %' + ' \n'

        return msg

if __name__ == '__main__':
    time_start = time.time()
    go = TimeStrategyEquityBondYield()
    result = go.generate_today_notification_msg()
    print(result)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))