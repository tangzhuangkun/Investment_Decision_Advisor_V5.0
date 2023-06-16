import time
import threading

import sys
sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger
import data_collector.get_target_real_time_indicator_from_interfaces as get_stock_real_time_indicator_from_interfaces
import db_mapper.target_pool.investment_target_mapper as investment_target_mapper
import db_mapper.financial_data.stocks_main_estimation_indexes_historical_data_mapper as stocks_main_estimation_indexes_historical_data_mapper


class StockStrategyMonitoringEstimation:
    # 股票的监控策略，当触发条件时，返回信息并通知
    # 频率：分钟级，每个交易日，盘中

    def __init__(self):
        pass


    def get_tracking_stocks_realtime_indicators_trigger_result_single_thread(self):
        # 获取所有跟踪股票的实时指标的对预设条件的触发结果, 单线程
        # 返回 所有触发了条件的股票及信息的字典
        # 如 {'sz000002': [('sz000002', '万科A', 'pb', '1.01 小于设定估值 1.1, 当前百分位 1.12% 小于触发值5.0%'), ('sz000002', '万科A', 'pe_ttm', '5.99 小于设定估值 6.0, ')], 'sh600048': [('sh600048', '保利发展', 'pb', '当前百分位 10.46% 小于触发值50.0%')]}

        # 保存所有触发了条件的股票
        triggered_stocks_info_dict = dict()

        # 获取标的池中跟踪关注股票及对应的估值方式和触发条件(估值，低于等于历史百分位)
        # 如，[{'stock_code': '000002', 'stock_name': '万科A', 'stock_code_with_init': 'sz000002', 'stock_code_with_market_code': '000002.XSHE', 'valuation_method': 'pb', 'trigger_value': Decimal('0.95'), 'trigger_percent': Decimal('0.50')}, {'stock_code': '600048', 'stock_name': '保利发展', 'stock_code_with_init': 'sh600048', 'stock_code_with_market_code': '600048.XSHG', 'valuation_method': 'pb', 'trigger_value': Decimal('0.89'), 'trigger_percent': Decimal('10.00')}, {'stock_code': '000002', 'stock_name': '万科A', 'stock_code_with_init': 'sz000002', 'stock_code_with_market_code': '000002.XSHE', 'valuation_method': 'pe_ttm', 'trigger_value': Decimal('6.00'), 'trigger_percent': Decimal('5.00')}]
        tracking_stocks_valuation_method_and_trigger_dict = investment_target_mapper.InvestmentTargetMapper().get_stocks_valuation_method_and_trigger("stock", "active", "buy", "minutely")

        for stock_info in tracking_stocks_valuation_method_and_trigger_dict:
            # 股票代码，如 000002
            stock_code = stock_info.get("stock_code")
            # 含股票上市地的代码, 如 sz000002
            stock_code_with_location = stock_info.get("stock_code_with_init")
            # 股票名称，如 万科A
            stock_name = stock_info.get("stock_name")
            # 估值方式，如 pb,pe_ttm, dr_ttm
            estimation_method = stock_info.get("valuation_method")
            # 估值触发条件的值, 如 0.95
            trigger_value = float(stock_info.get("trigger_value"))
            # 估值触发条件的历史百分位（低于等于某百分位）， 如 0.5
            trigger_percent = float(stock_info.get("trigger_percent"))

            # 如果触发条件，result 返回 股票上市地的代码, 中文名称, 估值方式, 估值, 提示信息
            # 如果未触发条件，result 返回 None
            result = self.compare_realtime_estimation_with_triggers(stock_code, stock_code_with_location, stock_name, estimation_method, trigger_value, trigger_percent)
            # 如果返回的结果不为空
            if result != None:
                # 纳入属于触发预设条件的股票范围
                # 如果之前未保存过该股票的触发条件返回信息
                if stock_code_with_location not in triggered_stocks_info_dict:
                    value_list = []
                    value_list.append(result)
                    triggered_stocks_info_dict[stock_code_with_location] = value_list
                # 如果之前已保存过该股票的触发条件返回信息
                else:
                    triggered_stocks_info_dict[stock_code_with_location].append(result)

            # 如果有返回信息
        if len(triggered_stocks_info_dict) != 0:
            # 日志记录
            log_msg = '获取所有跟踪股票的实时指标的对预设条件的触发结果为' + str(triggered_stocks_info_dict)
            custom_logger.CustomLogger().log_writter(log_msg, 'info')

            # 返回 所有触发了条件的股票
        return triggered_stocks_info_dict


    def compare_realtime_estimation_with_triggers(self, stock_code, stock_code_with_location, stock_name, estimation_method, trigger_value, trigger_percent):
        # 对比获取到的估值与触发条件做对比

        # stock_code, 股票代码，如 000002
        # stock_code_with_location, 含股票上市地的代码, 如 sz000002
        # stock_name, 股票名称，如 万科A
        # estimation_method, 估值方式，如 pe_ttm, pb, dr_ttm
        # trigger_value, 估值触发条件的值, 如 0.95
        # trigger_percent, 估值触发条件的历史百分位（低于等于某百分位）， 如 0.5
        # return:
        # 如果触发条件，返回 股票上市地的代码, 中文名称, 估值方式, 估值, 提示信息
        # 如果未触发条件，返回 None

        # 关于触发值的信息
        trigger_value_msg = ""
        # 关于触发值历史百分位的信息
        trigger_percent_msg = ""
        # 关于触发值的总结
        trigger_conclusion = ""

        # 从腾讯接口获取实时估值数据
        realtime_estimation = get_stock_real_time_indicator_from_interfaces.GetTargetRealTimeIndicatorFromInterfaces().get_single_target_real_time_indicator(
           stock_code_with_location, estimation_method)
        realtime_estimation = float(realtime_estimation)

        # 如果实时估值小于等于触发条件
        if( realtime_estimation<=trigger_value):
            trigger_value_msg = str(realtime_estimation)+" 小于设定估值 "+str(trigger_value)+", "

        # 从数据库获取该股票的历史估值数据
        # 如 [{'pb': Decimal('0.9009277645423478')}, {'pb': Decimal('0.9101209049968616')}, ，，，]
        # 或者 [{'pe_ttm': Decimal('0.9009277645423478')}, {'pe_ttm': Decimal('0.9101209049968616')},, ，，，]
        #historical_estimation_list = self.get_stock_historical_estimation_from_db(stock_code, estimation_method)
        historical_estimation_list = stocks_main_estimation_indexes_historical_data_mapper.StocksMainEstimationIndexesHistoricalDataMapper().get_stock_all_historical_estimation(stock_code, estimation_method)
        # 如果实时估值小于历史最小值
        if (realtime_estimation <= float(historical_estimation_list[0][estimation_method])):
            # 返回 股票上市地的代码, 中文名称, 估值方式, 估值, 提示信息
            # 如 ('sz000002', '万科A', 'pb', 0.8, '0.8 小于设定估值 0.9, 且估值处于历史最低值')
            trigger_percent_msg = "估值处于历史最低值"
            trigger_conclusion = trigger_value_msg+"且"+trigger_percent_msg
            return stock_code_with_location,stock_name,estimation_method,realtime_estimation, trigger_conclusion

        # 遍历股票的历史估值数据
        for i in range(len(historical_estimation_list)):
            # 如果历史估值中某个数据大于等于实时估值
            if (float(historical_estimation_list[i][estimation_method])>=realtime_estimation):
                # 计算处于历史百分位
                percent = round(i/len(historical_estimation_list)*100,2)
                # 如果小于等于设定估值百分位
                if (percent<=trigger_percent):
                    # 触发 估值绝对值 和 触发历史百分位值
                    # 返回 股票上市地的代码, 中文名称, 估值方式, 估值, 提示信息
                    # 如 ('sz000002', '万科A', 'pb', 1.01, '1.01 小于设定估值 1.1, 当前百分位 1.12% 小于触发值5.0%')
                    # 或者 ('sh600048', '保利发展', 'pb', 1.14, '当前百分位 10.46% 小于触发值50.0%')
                    trigger_percent_msg = "当前百分位 "+str(percent)+"%"+" 小于触发值" + str(trigger_percent) + "%"
                    trigger_conclusion = trigger_value_msg+trigger_percent_msg
                    return stock_code_with_location, stock_name, estimation_method, realtime_estimation, trigger_conclusion
                else:
                    if trigger_value_msg == "":
                        return None
                    else:
                        # 仅触发 估值绝对值，未触发历史百分位值
                        # 返回 股票上市地的代码, 中文名称, 估值方式, 估值, 提示信息
                        # 如 ('sz000002', '万科A', 'pb', 0.9, '0.9 小于设定估值 1.1, ')
                        trigger_conclusion = trigger_value_msg
                        return stock_code_with_location, stock_name, estimation_method, realtime_estimation, trigger_conclusion
        return None



    def get_tracking_stocks_realtime_indicators_trigger_result_multi_threads(self):
        # 获取所有跟踪股票的实时指标的对预设条件的触发结果, 多线程
        # 返回 所有触发了条件的股票及信息的字典
        # 如 {'sz000002': [('sz000002', '万科A', 'pb', '1.01 小于设定估值 1.1, 当前百分位 1.12% 小于触发值5.0%'), ('sz000002', '万科A', 'pe_ttm', '5.99 小于设定估值 6.0, ')], 'sh600048': [('sh600048', '保利发展', 'pb', '当前百分位 10.46% 小于触发值50.0%')]}

        # 保存所有触发了条件的股票
        triggered_stocks_info_dict = dict()

        # 获取标的池中跟踪关注股票及对应的估值方式和触发条件(估值，低于等于历史百分位)
        # 如，[{'stock_code': '000002', 'stock_name': '万科A', 'stock_code_with_init': 'sz000002', 'stock_code_with_market_code': '000002.XSHE', 'valuation_method': 'pb', 'trigger_value': Decimal('0.95'), 'trigger_percent': Decimal('0.50')}, {'stock_code': '600048', 'stock_name': '保利发展', 'stock_code_with_init': 'sh600048', 'stock_code_with_market_code': '600048.XSHG', 'valuation_method': 'pb', 'trigger_value': Decimal('0.89'), 'trigger_percent': Decimal('10.00')}, {'stock_code': '000002', 'stock_name': '万科A', 'stock_code_with_init': 'sz000002', 'stock_code_with_market_code': '000002.XSHE', 'valuation_method': 'pe_ttm', 'trigger_value': Decimal('6.00'), 'trigger_percent': Decimal('5.00')}]
        tracking_stocks_valuation_method_and_trigger_dict = investment_target_mapper.InvestmentTargetMapper().get_stocks_valuation_method_and_trigger("stock", "active", "buy", "minutely")

        # 启用多线程
        running_threads = []
        # 启用线程锁
        threadLock = threading.Lock()

        for stock_info in tracking_stocks_valuation_method_and_trigger_dict:
            # 股票代码，如 000002
            stock_code = stock_info.get("stock_code")
            # 含股票上市地的代码, 如 sz000002
            stock_code_with_location = stock_info.get("stock_code_with_init")
            # 股票名称，如 万科A
            stock_name = stock_info.get("stock_name")
            # 估值方式，如 pb,pe_ttm, dr_ttm
            estimation_method = stock_info.get("valuation_method")
            # 估值触发条件的值, 如 0.95
            trigger_value = float(stock_info.get("trigger_value"))
            # 估值触发条件的历史百分位（低于等于某百分位）， 如 0.5
            trigger_percent = float(stock_info.get("trigger_percent"))

            # 启动线程
            running_thread = threading.Thread(target=self.get_single_tracking_stock_realtime_indicators,
                                              kwargs={"stock_code": stock_code,
                                                      "stock_code_with_location": stock_code_with_location,
                                                      "stock_name":stock_name,
                                                      "estimation_method":estimation_method,
                                                      "trigger_value":trigger_value,
                                                      "trigger_percent":trigger_percent,
                                                      "triggered_stocks_info_dict": triggered_stocks_info_dict,
                                                      "threadLock": threadLock})
            # 添加线程
            running_threads.append(running_thread)

            # 开启新线程
        for mem in running_threads:
            mem.start()

            # 等待所有线程完成
        for mem in running_threads:
            mem.join()

            # 如果有返回信息
        if len(triggered_stocks_info_dict) != 0:
            # 日志记录
            # Todo 此日志被多次触发，需要排查
            log_msg = '获取所有跟踪股票的实时指标的对预设条件的触发结果为' + str(triggered_stocks_info_dict)
            custom_logger.CustomLogger().log_writter(log_msg, 'info')

            # 所有触发了条件的股票返回信息
        return triggered_stocks_info_dict



    def get_single_tracking_stock_realtime_indicators(self, stock_code, stock_code_with_location, stock_name, estimation_method, trigger_value, trigger_percent, triggered_stocks_info_dict, threadLock):
        # 获取单个跟踪股票，估值策略下的返回信息

        # stock_code, 股票代码，如 '000002'
        # stock_code_with_location, 含股票上市地的代码, 如 sz000002
        # stock_name, 股票名称，如 万科A
        # estimation_method, 估值方式，如 pe_ttm, pb, dr_ttm
        # trigger_value, 估值触发条件的值, 如 0.95
        # trigger_percent, 估值触发条件的历史百分位（低于等于某百分位）， 如 0.5
        # riggered_stocks_info_dict, 保存所有触发了条件的股票，一个dict
        # threadLock，线程锁，用于线程同步, 向 triggered_stocks_info_list添加东西时，避免冲突

        # 无返回，将该跟踪股票下，估值策略下的返回信息添加进triggered_stocks_info_list

        # 如果触发条件，result 返回 股票上市地的代码, 中文名称, 估值方式, 估值, 提示信息
        # 如果未触发条件，result 返回 None
        result = self.compare_realtime_estimation_with_triggers(stock_code, stock_code_with_location, stock_name, estimation_method, trigger_value, trigger_percent)
        # 如果返回的结果不为空
        if result != None:
            # 纳入属于触发预设条件的股票范围

            # 获取锁，用于线程同步
            threadLock.acquire()
            # 如果之前未保存过该股票的触发条件返回信息
            if stock_code_with_location not in triggered_stocks_info_dict:
                value_list = []
                value_list.append(result)
                triggered_stocks_info_dict[stock_code_with_location] = value_list
            # 如果之前已保存过该股票的触发条件返回信息
            else:
                triggered_stocks_info_dict[stock_code_with_location].append(result)
            # 释放锁，开启下一个线程
            threadLock.release()


    def main(self):
        # 多线程，获取所有跟踪股票的实时指标的对预设条件的触发结果
        return self.get_tracking_stocks_realtime_indicators_trigger_result_multi_threads()



if __name__ == '__main__':
    time_start = time.time()
    go = StockStrategyMonitoringEstimation()
    result = go.main()
    #result = go.compare_realtime_estimation_with_triggers('000002','sz000002','万科A','pb',0.9,10)
    #result = go.get_tracking_stocks_realtime_indicators_trigger_result_multi_threads()
    #result = go.get_tracking_stocks_realtime_indicators_trigger_result_single_thread()
    print(result)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))