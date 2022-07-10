import decimal
import time
import sys
import threading

sys.path.append("..")
import database.db_operator as db_operator
import data_collector.get_stock_real_time_indicator_from_interfaces as get_stock_real_time_indicator_from_interfaces
import log.custom_logger as custom_logger
import data_miner.data_miner_common_index_operator as data_miner_common_index_operator
import data_miner.data_miner_common_db_operator as data_miner_common_db_operator
import data_collector.data_collector_common_index_collector as data_collector_common_index_collector
import data_miner.data_miner_common_target_index_operator as data_miner_common_target_index_operator


class FundStrategyPBEstimation:
    # 指数基金策略，市净率率估值法
    # 用于周期性行业
    # 频率：每个交易日，盘中

    def __init__(self):
        pass

    def get_stock_historical_pb(self, stock_code, day):
        # 提取股票的历史某一天的市净率信息， 包括市净率, 扣商誉市净率
        # param: stock_code, 股票代码，如 000596
        # param: day, 日期， 如 2020-09-01
        # 返回：市净率, 扣商誉市净率
        selecting_sql = "select pb, pb_wo_gw from stocks_main_estimation_indexes_historical_data where stock_code = '%s'" \
                        " and date = '%s' " % (stock_code, day)
        pb_info = db_operator.DBOperator().select_all("financial_data", selecting_sql)
        return pb_info

    '''
    def calculate_a_historical_date_index_PB(self, index_code, day):
        # 废弃，应该从已算好的数据库中获取某个历史日期的市净率
        # 基于当前指数构成，计算过去某一天该指数市净率, 扣商誉市净率
        # param: index_code 指数代码，如 399997 或者 399997.XSHE
        # param: day, 日期， 如 2020-09-01
        # 返回 指数市净率, 扣商誉市净率， 如 1.510749960655767552952 1.541282892742027267960
        pb = 0
        pb_wo_gw = 0

        # 获取指数成分股及权重
        index_constitute_stocks_weight = data_miner_common_index_operator.DataMinerCommonIndexOperator().get_index_constitute_stocks(index_code)
        for stock_info in index_constitute_stocks_weight:
            # 获取指数市净率信息
            pb_info = self.get_stock_historical_pb(stock_info['stock_code'], stock_info['stock_name'], day)
            # 如果能获取到信息
            if len(pb_info)!=0:
                # 计算市净率, 扣商誉市净率
                pb += decimal.Decimal(pb_info[0]["pb"])*decimal.Decimal(stock_info["weight"])/100
                pb_wo_gw += decimal.Decimal(pb_info[0]["pb_wo_gw"])*decimal.Decimal(stock_info["weight"])/100
        return round(pb,4), round(pb_wo_gw,4)
    '''

    def get_a_historical_date_index_PB(self, index_code, day):
        # 从已算好的数据库中获取某个历史日期的市净率
        # param: index_code 指数代码，399997
        # param: day, 日期， 如 2020-09-01
        # 返回 指数市净率, 扣商誉市净率
        # 如 (Decimal('56.39008'), Decimal('53.32287'))
        selecting_sql = "select pb, pb_wo_gw from index_components_historical_estimations" \
                        " where index_code = '%s' and historical_date = '%s' " % (index_code, day)
        index_pb_info = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        return index_pb_info['pb'], index_pb_info['pb_wo_gw']

    '''
    def calculate_index_pb_in_a_period_time(self, index_code, n_year):
        # 基于当前成分股和比例，计算该指数过去x年，每一天的市净率，扣商誉市净率
        # param: index_code 指数代码，如 399997
        # 返回 每一天的指数市净率, 扣商誉市净率
        # 获取今天日期

        # todo 未完
        now_time = datetime.date.today()
        # 获取n年前的起始日期
        last_n_year = now_time + datetime.timedelta(days=-round(365*n_year))
        # 遍历这段时间
        for i in range((now_time - last_n_year).days + 1):
            specific_day = last_n_year + datetime.timedelta(days=i)
            pb, pb_wo_gw = self.calculate_a_historical_date_index_PB(index_code, str(specific_day))
            print(specific_day, pb, pb_wo_gw)
    '''

    def get_and_calculate_single_stock_pb_weight_in_index(self, stock_id, stock_weight, threadLock):
        # 获取并计算单个股票市净率在指数中的权重
        # stock_id: 股票代码（2位上市地+6位数字， 如 sz000596）
        # stock_weight： 默认参数，在指数中，该成分股权重，默认为0
        # threadLock：线程锁

        # 通过抓取数据雪球页面，获取单个股票的实时滚动市净率
        # stock_real_time_pb = xueqiu.GetStockRealTimeIndicatorFromXueqiu().get_single_stock_real_time_indicator(stock_id, 'pb')
        # 从腾讯接口获取实时市净率估值数据
        stock_real_time_pb = get_stock_real_time_indicator_from_interfaces.GetStockRealTimeIndicatorFromInterfaces().get_single_stock_real_time_indicator(
            stock_id, 'pb')

        # 获取锁，用于线程同步
        threadLock.acquire()
        # 统计指数的实时市净率，成分股权重*股票实时的市净率
        self.index_real_time_pb += stock_weight * decimal.Decimal(stock_real_time_pb)
        # 释放锁，开启下一个线程
        threadLock.release()

    def calculate_real_time_index_pb_multiple_threads(self, index_code):
        # 多线程计算指数的实时市净率
        # index_code, 指数代码， 如 399997
        # 输出，指数的实时市净率， 如 18.5937989

        # 统计指数实时的市净率
        self.index_real_time_pb = 0

        # 获取指数的成分股和权重
        # [{'stock_code': '000568', 'stock_name': '泸州老窖', 'weight': Decimal('15.487009751893797000'), 'stock_exchange_location': 'sz', 'stock_market_code': 'XSHE'},
        #         # {'stock_code': '000596', 'stock_name': '古井贡酒', 'weight': Decimal('3.438504576505159600'), 'stock_exchange_location': 'sz', 'stock_market_code': 'XSHE'},,,,]
        stocks_and_their_weights = data_miner_common_index_operator.DataMinerCommonIndexOperator().get_index_constitute_stocks(
            index_code)

        # 启用多线程
        running_threads = []
        # 启用线程锁
        threadLock = threading.Lock()

        # 遍历指数的成分股
        for i in range(len(stocks_and_their_weights)):
            # 拼接股票代码，例如 sh600726

            # 获取成分股上市地，
            # sh 代表上海
            # sz 代表深圳
            stock_exchange_location = stocks_and_their_weights[i]["stock_exchange_location"]
            # 获取成分股代码，6位纯数字， 000568
            stock_code = stocks_and_their_weights[i]["stock_code"]
            # 获取成分股权重,
            stock_weight = stocks_and_their_weights[i]["weight"]

            # 将股票代码进行转换，如 sz000568
            stock_id = stock_exchange_location + stock_code

            # 启动线程
            running_thread = threading.Thread(target=self.get_and_calculate_single_stock_pb_weight_in_index,
                                              kwargs={"stock_id": stock_id, "stock_weight": stock_weight,
                                                      "threadLock": threadLock})
            running_threads.append(running_thread)

        # 开启新线程
        for mem in running_threads:
            mem.start()

        # 等待所有线程完成
        for mem in running_threads:
            mem.join()

        # 整体市净率除以100，因为之前的权重没有除以100
        self.index_real_time_pb = self.index_real_time_pb / 100

        # 获取指数名称
        index_name = data_miner_common_index_operator.DataMinerCommonIndexOperator().get_index_name(index_code)

        # 日志记录
        log_msg = '已获取 ' + index_name + ' 实时 PB'
        custom_logger.CustomLogger().log_writter(log_msg, 'info')

        return round(self.index_real_time_pb, 5)

    def get_last_trading_day_PB(self, index_code):
        # 获取当前指数上一个交易日的  市净率 和 扣商誉市净率
        # index_code: 指数代码, 必须如 399965
        # return： 如果有值，则返回tuple，( PB , 扣商誉市净率)
        #          如： (Decimal('16.581174467'), Decimal('16.616582865'))
        #          如果无值，则返回tuple，(0, 0)

        # 获取当前日期
        today = time.strftime("%Y-%m-%d", time.localtime())
        # today = "2021-06-17"
        the_last_trading_date = data_miner_common_db_operator.DataMinerCommonDBOperator().get_the_last_trading_date(
            today)

        # 获取指数上一个交易日的市净率 和 扣商誉市净率
        # 整体市净率除以有效权重得到有效市净率
        selecting_sql = "select pb/(pb_effective_weight/100) as pb, pb_wo_gw/(pb_wo_gw_effective_weight/100) as pb_wo_gw from " \
                        "index_components_historical_estimations where index_code = '%s' and historical_date = '%s'" \
                        % (index_code, the_last_trading_date)
        # index_pb_info 如 {'pb': Decimal('16.581174467'), 'pb_wo_gw': Decimal('16.616582865')}
        index_pb_info = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        # 如果pe_info为空
        if index_pb_info is not None:
            return (index_pb_info["pb"], index_pb_info["pb_wo_gw"])
        else:
            # 日志记录
            log_msg = "无法获取日期 " + today + " 的上一个交易日 " + the_last_trading_date + " 的市净率 和 扣商誉市净率数据"
            custom_logger.CustomLogger().log_writter(log_msg, 'error')
            return (0, 0)

    def cal_the_PB_percentile_in_history(self, index_code, index_code_with_location):
        # 获取当前指数的实时市净率， 并计算当前实时PB在历史上的百分位水平，预估扣商誉市净率，历史百分位，同比上个交易日涨跌幅
        # index_code: 指数代码, 必须如 399965
        # index_code_with_location: 指数代码（含上市地），sz399965
        # return: 当前指数的实时PB， 在历史上的百分位水平，预估的实时扣商誉市净率，历史百分位，同比上个交易日涨跌幅
        #         如 [Decimal('1.0617'), 0.0063, Decimal('1.1857'), 0.0237, Decimal('1.96')]

        # 返回的字段列表
        result_list = []

        # 获取指数历史上所有日期的市净率 和 扣商誉市净率
        # 整体市净率除以有效权重得到有效市净率
        selecting_sql = "select pb/(pb_effective_weight/100) as pb, pb_wo_gw/(pb_wo_gw_effective_weight/100) as pb_wo_gw" \
                        ", historical_date from index_components_historical_estimations where index_code = '%s' " \
                        "order by pb/(pb_effective_weight/100)" % (index_code)
        # {'pb': Decimal('2.534098060'), 'pb_wo_gw': Decimal('2.540534756'), 'historical_date': datetime.date(2014, 5, 20)}, {'pb': Decimal('2.538920014'), 'pb_wo_gw': Decimal('2.545312165'), 'historical_date': datetime.date(2014, 6, 4)},,,,]
        index_all_historical_pb_info_list = db_operator.DBOperator().select_all("aggregated_data", selecting_sql)

        pb_list = []
        pb_wo_gw_list = []
        # 获取 历史上 市净率 和 扣商誉市净率 的列表，由小到大排序
        for info_unit in index_all_historical_pb_info_list:
            # pb_list已经是由小到大排序的
            pb_list.append(info_unit["pb"])
            pb_wo_gw_list.append(info_unit["pb_wo_gw"])

        # 扣商誉市净率，由小到大排序
        pb_wo_gw_list.sort()
        # 获取实时的有效市净率
        index_real_time_effective_pb = self.calculate_real_time_index_pb_multiple_threads(index_code)
        # 返回结果，添加 当前指数的实时市净率
        result_list.append(index_real_time_effective_pb)

        # 返回结果，添加 当前指数的实时市净率在历史上的百分位
        # 如果历史上最小的市净率值都大于当前的实时值，即处于 0%
        if (pb_list[0] >= index_real_time_effective_pb):
            result_list.append(0)
        # 如果历史上最大的市净率值都小于当前的实时值，即处于 100%
        elif (pb_list[len(pb_list) - 1] < index_real_time_effective_pb):
            result_list.append(1)
        else:
            for i in range(len(pb_list)):
                # 如果历史上某个市净率值大于当前的实时值，则返回其位置
                if (pb_list[i] >= index_real_time_effective_pb):
                    result_list.append(round(i / len(pb_list), 5))
                    break

        # 获取上个交易日的收盘 市净率 和 扣商誉市净率
        last_trading_day_pb, last_trading_day_pb_wo_gw = self.get_last_trading_day_PB(index_code)

        # 获取指数最新的涨跌率
        index_latest_increasement_decreasement_rate = data_collector_common_index_collector.DataCollectorCommonIndexCollector().get_target_latest_increasement_decreasement_rate(
            index_code_with_location)

        # 根据市净率的同比涨跌幅，预估实时的扣商誉市净率
        index_real_time_predictive_pb_wo_gw = round(
            last_trading_day_pb_wo_gw * (1 + index_latest_increasement_decreasement_rate / 100), 5)

        # 返回结果，添加 当前指数的预估的实时扣商誉市净率
        result_list.append(index_real_time_predictive_pb_wo_gw)

        # 返回结果，添加 当前指数的预估的实时扣商誉市净率在历史上的百分位
        # 如果历史上最小的扣商誉市净率值都大于当前的实时预估值，即处于 0%
        if (pb_wo_gw_list[0] >= index_real_time_predictive_pb_wo_gw):
            result_list.append(0)
        # 如果历史上最大的扣商誉市净率值都小于当前的实时预估值，即处于 100%
        elif (pb_wo_gw_list[len(pb_wo_gw_list) - 1] < index_real_time_predictive_pb_wo_gw):
            result_list.append(1)
        # 如果处于 0% - 100%
        else:
            for i in range(len(pb_wo_gw_list)):
                # 如果历史上某个扣商誉市净率值大于当前的实时预估值，则返回其位置
                if (pb_wo_gw_list[i] >= index_real_time_predictive_pb_wo_gw):
                    result_list.append(round(i / len(pb_wo_gw_list), 5))
                    break

        # 返回结果，添加 同比上个交易日涨跌幅
        result_list.append(index_latest_increasement_decreasement_rate)

        return result_list

    # def calculate_all_tracking_index_funds_real_time_PB_and_generate_msg(self):
    def generate_PB_strategy_msg(self):
        # 组装市净率策略信息
        # return: 返回计算结果

        # 获取标的池中跟踪关注指数及他们的中文名称
        '''
        # 字典形式。如，{'399396.XSHE': '国证食品', '000932.XSHG': '中证主要消费',,,,}
        # indexes_and_their_names = read_collect_target_fund.ReadCollectTargetFund().get_indexes_and_their_names()
        # indexes_and_their_names = read_collect_target_fund.ReadCollectTargetFund().index_valuated_by_method('pb')
        '''
        # [{'index_code': '399965', 'index_name': '中证800地产', 'index_code_with_init': 'sz399965', 'index_code_with_market_code': '399965.XSHE'},，，]
        indexes_and_their_names = data_miner_common_target_index_operator.DataMinerCommonTargetIndexOperator().index_valuated_by_method('pb')

        # 拼接需要发送的指数实时市净率信息
        indexes_and_real_time_PB_msg = '指数实时市净率和自2010年来历史百分位： \n\n'
        for index_code_info in indexes_and_their_names:
            # 获取 当前指数的实时市净率， 在历史上的百分位水平，预估的实时扣商誉市净率，历史百分位，同比上个交易日涨跌幅
            # 如 [Decimal('1.0617'), 0.0063, Decimal('1.1857'), 0.0237, Decimal('1.96')]
            pb_result_list = self.cal_the_PB_percentile_in_history(index_code_info["index_code"],index_code_info["index_code_with_init"])
            # 生成 讯息
            indexes_and_real_time_PB_msg += index_code_info["index_name"] + ":  \n" + "同比上一个交易日: " + str(
                pb_result_list[4]) + "%;" + "\n" + "--------" + "\n" + "实时市净率: " + \
                                            str(pb_result_list[0]) + "\n" + "历史百分位: " + str(
                decimal.Decimal(pb_result_list[1] * 100).quantize(decimal.Decimal('0.00'))) + "%;" + "\n" + \
                                            "--------" + "\n" + "预估扣商誉市净率: " + str(
                pb_result_list[2]) + "\n" + "历史百分位: " + \
                                            str(decimal.Decimal(pb_result_list[3] * 100).quantize(
                                                decimal.Decimal('0.00'))) + "%;\n"+"***************"+ "\n\n"

        # 日志记录
        log_msg = '市净率策略执行完毕'
        custom_logger.CustomLogger().log_writter(log_msg, 'debug')
        return indexes_and_real_time_PB_msg


if __name__ == '__main__':
    time_start = time.time()
    go = FundStrategyPBEstimation()
    # result = go.get_index_constitute_stocks("399965")
    # print(result)
    #result = go.get_stock_historical_pb("000002", "2021-12-24")
    #print(result)
    #pb, pb_wo_gw = go.get_a_historical_date_index_PB("399997", "2021-12-24")
    #print(pb, pb_wo_gw)
    #result = go.get_last_trading_day_PB("399997")
    #print(result)
    # go.calculate_index_pb_in_a_period_time("399965",0.5)
    #result = go.cal_the_PB_percentile_in_history("399965", "sz399965")
    # result = go.cal_the_PB_percentile_in_history("399965.XSHE")
    # result = go.calculate_real_time_index_pb_multiple_threads("399965.XSHE")
    #print(result)
    msg = go.generate_PB_strategy_msg()
    print(msg)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)
