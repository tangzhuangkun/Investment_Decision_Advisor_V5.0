import time
import sys
sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger
import data_collector.collect_chn_gov_bonds_rates as collect_chn_gov_bonds_rates
import data_collector.collect_index_estimation_from_lxr as collect_index_estimation_from_lxr
import data_miner.data_miner_common_db_operator as data_miner_common_db_operator
import data_miner.calculate_stock_bond_ratio as calculate_stock_bond_ratio

class TimeStrategyEquityBondYield:
    # 择时策略，股债收益率
    # 沪深300指数市值加权估值PE/十年国债收益率
    # 用于判断股市收益率与无风险收益之间的比值
    # 频率：每个交易日，盘后

    def __init__(self):
        pass

    def prepare_index_estimation_and_bond_rate(self):
        # 准备数据，收集最新沪深300指数市值加权估值和国债利率

        # 收集最新国债收益率
        collect_chn_gov_bonds_rates.CollectCHNGovBondsRates().main()
        # 收集最新沪深300指数市值加权估值
        collect_index_estimation_from_lxr.CollectIndexEstimationFromLXR().main()


    def cal_the_ratio_percentile_in_history(self):
        # 计算最新交易日期市盈率，10年期国债收益率，股债比，及历史百分位
        # 返回：如 {'trading_date': '2021-11-26', 'pe': Decimal('13.0141'), 'bond': Decimal('2.8200'), 'ratio': Decimal('2.7248'), 'percent': 74.47}

        # 获取当前日期
        today = time.strftime("%Y-%m-%d", time.localtime())

        # 获取最新交易日期
        lastest_trading_date = str(data_miner_common_db_operator.DataMinerCommonDBOperator().get_the_lastest_trading_date(today))

        # 返回的字典
        today_info_dict = {}

        # 获取 交易日期和对应的股债比，按股债比从大到小排列
        selecting_sql = 'select trading_date, round(pe,4) as pe, round(10y_bond_rate*100,4) as bond, round(ratio,4) as ' \
                        'ratio from stock_bond_ratio_di order by ratio'
        trading_date_and_ratio_list = db_operator.DBOperator().select_all( "aggregated_data", selecting_sql)

        # 有多长的时间
        date_counter = len(trading_date_and_ratio_list)

        for i in range(len(trading_date_and_ratio_list)):
            # 日期与最新交易日期一致
            # 股债比，及历史百分位 均只保留4位小数
            if str(trading_date_and_ratio_list[i]["trading_date"]) == str(lastest_trading_date):
                # 返回中存入最新交易日期日期
                today_info_dict["trading_date"] = trading_date_and_ratio_list[i]["trading_date"]
                # 返回中存入最新交易日期沪深300的市盈率
                today_info_dict["pe"] = trading_date_and_ratio_list[i]["pe"]
                # 返回中存入最新交易日期国债收益率
                today_info_dict["bond"] = trading_date_and_ratio_list[i]["bond"]
                # 返回中存入最新交易日期股债收益比
                today_info_dict["ratio"] = trading_date_and_ratio_list[i]["ratio"]

                # 返回中存入所处历史百分位
                # 处于历史最大值
                if(i==date_counter-1):
                    today_info_dict["percent"] = 1
                # 处于历史极小值
                elif (i==0):
                    today_info_dict["percent"] = 0
                # 处于中间某个值
                else:
                    today_info_dict["percent"] = round(i / date_counter,4)*100
                return today_info_dict

    def generate_strategy_msg(self):
        # 生成通知信息
        '''
        返回：
        2021-11-26:
        股债比: 2.7248
        自2010年百分位: 74.47%
        沪深300市盈率: 13.0141
        国债收益率: 2.8200
        '''

        # 最新交易日期市盈率，10年期国债收益率，股债比，及历史百分位信息
        # {'trading_date': '2021-11-26', 'pe': Decimal('13.0141'), 'bond': Decimal('2.8200'), 'ratio': Decimal('2.7248'), 'percent': 0.7447}
        today_info_dict = self.cal_the_ratio_percentile_in_history()

        msg = ''
        # 如果股债收益比大于等于3 或者 大于等于历史百分位94
        if today_info_dict['ratio'] >= 3 or today_info_dict['percent']>=94:
            msg += "需特别注意，已进入重点投资区间\n\n"
        msg += str(today_info_dict['trading_date']) + ': \n'
        msg += '股债比: ' + str(today_info_dict['ratio']) + '\n'
        msg += '自2010年百分位: ' + str(round(today_info_dict['percent'],6))+'%' + '\n'
        msg += '沪深300市盈率: ' + str(today_info_dict['pe']) + '\n'
        msg += '国债收益率: ' + str(today_info_dict['bond']) + '\n'
        return msg

    def main(self):
        self.prepare_index_estimation_and_bond_rate()
        # 运行mysql脚本，计算股债收益率
        calculate_stock_bond_ratio.CalculateStockBondRatio().main()
        msg = self.generate_strategy_msg()
        return msg


if __name__ == '__main__':
    time_start = time.time()
    go = TimeStrategyEquityBondYield()
    msg = go.main()
    print(msg)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))