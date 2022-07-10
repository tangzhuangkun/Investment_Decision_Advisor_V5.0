import decimal
import time
import threading

import sys
sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger
import data_miner.data_miner_common_index_operator as data_miner_common_index_operator
import data_miner.data_miner_common_db_operator as data_miner_common_db_operator
import data_collector.data_collector_common_index_collector as data_collector_common_index_collector
import data_collector.get_stock_real_time_indicator_from_interfaces as get_stock_real_time_indicator_from_interfaces
import data_miner.data_miner_common_target_index_operator as data_miner_common_target_index_operator


class FundStrategyPEEstimation:
    # 指数基金策略，市盈率估值法
    # 用于非周期性行业
    # 频率：每个交易日，盘中

    def __init__(self):
        pass

    def get_stock_historical_pe(self, stock_code, day):
        # 提取股票的历史市盈率信息， 包括市盈率TTM, 扣非市盈率TTM
        # param: stock_code, 股票代码，如 000596
        # param: day, 日期， 如 2020-09-01
        # 返回：市盈率TTM, 扣非市盈率TTM
        # 例如 [{'pe_ttm': Decimal('61.6921425379745000'), 'pe_ttm_nonrecurring': Decimal('65.5556071829405200')}]
        selecting_sql = "select pe_ttm, pe_ttm_nonrecurring from stocks_main_estimation_indexes_historical_data " \
                        "where stock_code = '%s' and date = '%s' " % (stock_code,day)
        pe_info = db_operator.DBOperator().select_all("financial_data", selecting_sql)
        return pe_info

    '''
    def calculate_a_historical_date_index_PE(self, index_code, day):
        # 废弃，应该从已算好的数据库中获取某个历史日期的市盈率 
        # 基于当前指数构成，计算过去某一天该指数市盈率TTM, 扣非市盈率TTM
        # param: index_code 指数代码，如 399997 或者 399997.XSHE
        # param: day, 日期， 如 2020-09-01
        # 返回 指数市盈率TTM, 扣非市盈率TTM, 均保留4位小数,
        # 例如，8.181 10.281
        pe_ttm = 0
        pe_ttm_nonrecurring = 0

        # 获取指数成分股及权重
        index_constitute_stocks_weight = data_miner_common_index_operator.DataMinerCommonIndexOperator().get_index_constitute_stocks(index_code)
        for stock_info in index_constitute_stocks_weight:
            # 获取指数市盈率信息
            pe_info = self.get_stock_historical_pe(stock_info['stock_code'], stock_info['stock_name'], day)
            # 计算市盈率TTM, 扣非市盈率TTM，仅考虑为正的情况
            if pe_info[0]["pe_ttm"] >= 0:
                pe_ttm += decimal.Decimal(pe_info[0]["pe_ttm"])*decimal.Decimal(stock_info["weight"])/100
            if pe_info[0]["pe_ttm_nonrecurring"] >=0:
                pe_ttm_nonrecurring += decimal.Decimal(pe_info[0]["pe_ttm_nonrecurring"])*decimal.Decimal(stock_info["weight"])/100
        return round(pe_ttm,4), round(pe_ttm_nonrecurring,4)
    '''

    def get_a_historical_date_index_PE(self, index_code, day):
        # 从已算好的数据库中获取某个历史日期的市盈率
        # param: index_code 指数代码，399997
        # param: day, 日期， 如 2020-09-01
        # 返回 指数市盈率TTM, 扣非市盈率TTM
        # 如 (Decimal('56.39008'), Decimal('53.32287'))
        selecting_sql = "select pe_ttm, pe_ttm_nonrecurring from index_components_historical_estimations" \
                        " where index_code = '%s' and historical_date = '%s' " % (index_code, day)
        index_pe_info = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        return index_pe_info['pe_ttm'],index_pe_info['pe_ttm_nonrecurring']

    def is_a_number(self, input_str):
        # 判断字符串是否为数字，int，float，负数（如：1，-32.34，-45, 34.65）
        # input_str: 输入的字符串
        # 为数字，输出 True; 不是数字，输出 False

        # 能成功转换为浮点型，则是数字
        try:
            float(input_str)
            return True
        except:
            return False

    def get_and_calculate_single_stock_pe_ttm_weight_in_index(self, stock_id, stock_weight, threadLock):
        # 获取并计算单个股票市盈率在指数中的权重
        # stock_id: 股票代码（2位上市地+6位数字， 如 sz000596）
        # stock_weight： 默认参数，在指数中，该成分股权重，默认为0
        # threadLock：线程锁

        '''
        # 通过抓取数据雪球页面，获取单个股票的实时滚动市盈率
        stock_real_time_pe_ttm = xueqiu.GetStockRealTimeIndicatorFromXueqiu().get_single_stock_real_time_indicator(stock_id, 'pe_ttm')
        # 如果获取的股票实时滚动市盈率不是数字，如’亏损‘
        if not self.is_a_number(stock_real_time_pe_ttm):
            # 股票实时滚动市盈率为0
            stock_real_time_pe_ttm = '0'
            # 忽略亏损的成分股票的权重
            stock_weight = 0
        '''
        # 从腾讯接口获取实时市净率估值数据
        stock_real_time_pe_ttm = get_stock_real_time_indicator_from_interfaces.GetStockRealTimeIndicatorFromInterfaces().get_single_stock_real_time_indicator(
            stock_id, 'pe_ttm')
        # 如果获取的股票实时滚动市盈率小于0，即’亏损‘
        if (decimal.Decimal(stock_real_time_pe_ttm)<0):
            # 股票实时滚动市盈率为0
            stock_real_time_pe_ttm = "0"
            # 忽略亏损的成分股票的权重
            stock_weight = 0

        # 获取锁，用于线程同步
        threadLock.acquire()
        # 统计指数的实时市盈率，成分股权重*股票实时的市盈率
        self.index_real_time_positive_pe_ttm += stock_weight * decimal.Decimal(stock_real_time_pe_ttm)
        # 累加有效（即为正值的）市盈率成分股权重
        self.index_positive_pe_ttm_weight += stock_weight
        # 释放锁，开启下一个线程
        threadLock.release()

    def calculate_real_time_index_pe_multiple_threads(self,index_code):
        # 多线程计算指数的实时市盈率
        # index_code, 指数代码,如 399997
        # 输出，指数的实时市盈率， 如 70.5937989

        # 统计指数实时的市盈率
        self.index_real_time_positive_pe_ttm = 0
        # 统计指数实时的市盈率为正数的权重合计
        self.index_positive_pe_ttm_weight = 0

        # 获取指数的成分股和权重
        stocks_and_their_weights = data_miner_common_index_operator.DataMinerCommonIndexOperator().get_index_constitute_stocks(index_code)

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
            running_thread = threading.Thread(target=self.get_and_calculate_single_stock_pe_ttm_weight_in_index,
                                              kwargs={"stock_id": stock_id, "stock_weight": stock_weight,
                                                      "threadLock": threadLock})
            running_threads.append(running_thread)

        # 开启新线程
        for mem in running_threads:
            mem.start()

        # 等待所有线程完成
        for mem in running_threads:
            mem.join()

        # 整体市盈率除以有效权重得到有效市盈率
        index_real_time_effective_pe_ttm = self.index_real_time_positive_pe_ttm/self.index_positive_pe_ttm_weight

        # 获取指数名称
        index_name = data_miner_common_index_operator.DataMinerCommonIndexOperator().get_index_name(index_code)

        # 日志记录
        log_msg = '已获取 ' + index_name + ' 实时 PE TTM'
        custom_logger.CustomLogger().log_writter(log_msg, 'info')

        # （市盈率为正值的成分股）累加市盈率/（市盈率为正值的成分股）有效权重
        return round(index_real_time_effective_pe_ttm,5)

    def get_last_trading_day_PE(self, index_code):
        # 获取当前指数上一个交易日的  PETTM 和 扣非市盈率
        # index_code: 指数代码, 必须如 399965
        # return： 如果有值，则返回tuple，(PETTM, 扣非市盈率)
        #          如果无值，则返回tuple，(0, 0)

        # 获取当前日期
        today = time.strftime("%Y-%m-%d", time.localtime())
        #today = "2021-06-17"
        the_last_trading_date = data_miner_common_db_operator.DataMinerCommonDBOperation().get_the_last_trading_date(today)

        # 获取指数上一个交易日的动态市盈率和扣非市盈率
        # 整体市盈率除以有效权重得到有效市盈率
        selecting_sql = "select pe_ttm/(pe_ttm_effective_weight/100) as pe_ttm, pe_ttm_nonrecurring/(pe_ttm_nonrecurring_effective_weight/100) as pe_ttm_nonrecurring from " \
                        "index_components_historical_estimations where index_code = '%s' and historical_date = '%s'" \
                        % (index_code, the_last_trading_date)
        # pe_info 如 {'pe_ttm': Decimal('61.27220'), 'pe_ttm_nonrecurring': Decimal('66.16330')}
        pe_info = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        # 如果pe_info为空
        if pe_info is not None:
            return (pe_info["pe_ttm"],pe_info["pe_ttm_nonrecurring"])

        else:
            # 日志记录
            log_msg = "无法获取日期 " + today +" 的上一个交易日 "+ the_last_trading_date+" 的PETTM 和 扣非市盈率数据"
            custom_logger.CustomLogger().log_writter(log_msg, 'error')
            return (0,0)

    def cal_the_PE_percentile_in_history(self, index_code, index_code_with_location):
        # 获取当前指数的实时PETTM， 并计算当前实时市盈率TTM在历史上的百分位水平，预估扣非市盈率，历史百分位，同比上个交易日涨跌幅
        # index_code: 指数代码, 如 399965
        # index_code_with_location: 指数代码（含上市地），sz399965
        # return: 当前指数的实时PETTM， 在历史上的百分位水平，预估的实时扣非市盈率，历史百分位，同比上个交易日涨跌幅
        #         如 [Decimal('58.7022'), 0.6686, Decimal('58.7844'), 0.3837, Decimal('0.0014')]

        # 返回的数列
        result_list = []

        # 获取指数历史上所有日期的动态市盈率和扣非市盈率
        # 整体市盈率除以有效权重得到有效市盈率
        selecting_sql = "select pe_ttm/(pe_ttm_effective_weight/100) as pe_ttm, pe_ttm_nonrecurring/(pe_ttm_nonrecurring_effective_weight/100) as pe_ttm_nonrecurring, historical_date from " \
                        "index_components_historical_estimations where index_code = '%s' " \
                        "order by pe_ttm/(pe_ttm_effective_weight/100)" %(index_code)
        index_all_historical_pe_info_list = db_operator.DBOperator().select_all("aggregated_data", selecting_sql)

        # 获取指数名称
        #index_name = index_operator.IndexOperator().get_index_name(index_code)

        pe_ttm_list = []
        pe_ttm_nonrecurring_list = []
        # 获取 历史上动态市盈率和扣非市盈率 的列表，由小到大排序
        for info_unit in index_all_historical_pe_info_list:
            # pe_ttm_list已经是由小到大排序的
            pe_ttm_list.append(info_unit["pe_ttm"])
            pe_ttm_nonrecurring_list.append(info_unit["pe_ttm_nonrecurring"])

        # 扣非市盈率，由小到大排序
        pe_ttm_nonrecurring_list.sort()
        # 获取实时的有效动态市盈率
        index_real_time_effective_pe_ttm = self.calculate_real_time_index_pe_multiple_threads(index_code)
        # 返回结果，添加 当前指数的实时PETTM
        result_list.append(index_real_time_effective_pe_ttm)

        # 返回结果，添加 当前指数的实时PETTM在历史上的百分位
        # 如果历史上最小的动态市盈率值都大于当前的实时值，即处于 0%
        if (pe_ttm_list[0] >= index_real_time_effective_pe_ttm):
            result_list.append(0)
        # 如果历史上最大的动态市盈率值都小于当前的实时值，即处于 100%
        elif (pe_ttm_list[len(pe_ttm_list) - 1] < index_real_time_effective_pe_ttm):
            result_list.append(1)
        # 如果处于 0% - 100%
        else:
            for i in range(len(pe_ttm_list)):
                # 如果历史上某个动态市盈率值大于当前的实时值，则返回其位置
                if(pe_ttm_list[i]>=index_real_time_effective_pe_ttm):
                    result_list.append(round(i / len(pe_ttm_list), 5))
                    break

        # 获取上个交易日的收盘PETTM，扣非市盈率
        last_trading_day_pe_ttm, last_trading_day_pe_ttm_nonrecurring = self.get_last_trading_day_PE(index_code)

        # 获取指数最新的涨跌率
        index_latest_increasement_decreasement_rate = data_collector_common_index_collector.DataCollectorCommonIndexCollector().get_target_latest_increasement_decreasement_rate(index_code_with_location)

        # 根据PETTM的同比涨跌幅，预估实时的扣非市盈率
        index_real_time_predictive_pe_ttm_nonrecurring = round(last_trading_day_pe_ttm_nonrecurring * (1 + index_latest_increasement_decreasement_rate/100), 5)

        # 返回结果，添加 当前指数的预估的实时扣非市盈率
        result_list.append(index_real_time_predictive_pe_ttm_nonrecurring)

        # 返回结果，添加 当前指数的预估的实时扣非市盈率在历史上的百分位
        # 如果历史上最小的扣非市盈率值都大于当前的实时预估值，即处于 0%
        if (pe_ttm_nonrecurring_list[0] >= index_real_time_predictive_pe_ttm_nonrecurring):
            result_list.append(0)
        # 如果历史上最大的扣非市盈率值都小于当前的实时预估值，即处于 100%
        elif (pe_ttm_nonrecurring_list[len(pe_ttm_nonrecurring_list) - 1] < index_real_time_predictive_pe_ttm_nonrecurring):
            result_list.append(1)
        else:
            for i in range(len(pe_ttm_nonrecurring_list)):
                # 如果历史上某个扣非市盈率值大于当前的实时预估值，则返回其位置
                if (pe_ttm_nonrecurring_list[i] >= index_real_time_predictive_pe_ttm_nonrecurring):
                    result_list.append(round(i / len(pe_ttm_nonrecurring_list), 5))
                    break

        # 返回结果，添加 同比上个交易日涨跌幅
        result_list.append(index_latest_increasement_decreasement_rate)

        return result_list

    def generate_PE_strategy_msg(self):
        # 返回包含 当前指数的实时PETTM， 在历史上的百分位水平，预估的实时扣非市盈率，历史百分位，同比上个交易日涨跌幅 的讯息
        # return: 返回讯息

        # 获取标的池中跟踪关注指数及他们的中文名称

        '''
        # 字典形式。如，{'399396.XSHE': '国证食品', '000932.XSHG': '中证主要消费',,,,}
        #indexes_and_their_names = read_collect_target_fund.ReadCollectTargetFund().get_indexes_and_their_names()
        #indexes_and_their_names = read_collect_target_fund.ReadCollectTargetFund().index_valuated_by_method('pe')
        #indexes_and_their_names = {'399396.XSHE': '国证食品'}
        '''

        # [{'index_code': '399965', 'index_name': '中证800地产', 'index_code_with_init': 'sz399965', 'index_code_with_market_code': '399965.XSHE'},，，]
        indexes_and_their_names = data_miner_common_target_index_operator.DataMinerCommonTargetIndexOperator().index_valuated_by_method(
            'pe_ttm')

        # 拼接需要发送的指数实时动态市盈率信息
        indexes_and_real_time_PE_msg = '指数实时动态市盈率和自2010年来历史百分位： \n\n'
        for index_code_info in indexes_and_their_names:
            # 获取 当前指数的实时PETTM， 在历史上的百分位水平，预估的实时扣非市盈率，历史百分位，同比上个交易日涨跌幅
            # 如 [Decimal('58.7022'), 0.6686, Decimal('58.7844'), 0.3837, Decimal('0.0014')]
            pe_result_list = self.cal_the_PE_percentile_in_history(index_code_info["index_code"],index_code_info["index_code_with_init"])
            # 生成 讯息
            indexes_and_real_time_PE_msg += index_code_info["index_name"] + ":  \n"+ "同比上一个交易日: "+str(pe_result_list[4])+"%;" +"\n"+"--------"+"\n"+"实时动态市盈率: "+\
                                            str(pe_result_list[0])+ "\n"+"历史百分位: "+str(decimal.Decimal(pe_result_list[1]*100).quantize(decimal.Decimal('0.00')))+"%;"+"\n"+\
                                            "--------"+"\n"+"预估扣非市盈率: "+str(pe_result_list[2])+ "\n"+"历史百分位: "+\
                                            str(decimal.Decimal(pe_result_list[3]*100).quantize(decimal.Decimal('0.00')))+"%;\n"+"***************"+ "\n\n"

        # 日志记录
        log_msg = '市盈率策略执行完毕'
        custom_logger.CustomLogger().log_writter(log_msg, 'debug')
        return indexes_and_real_time_PE_msg


if __name__ == '__main__':
    time_start = time.time()
    go = FundStrategyPEEstimation()
    result = go.generate_PE_strategy_msg()
    print(result)
    #result = go.get_index_constitute_stocks("399997")
    #print(result)
    #result = go.get_stock_historical_pe("000596",  "2021-12-24")
    #print(result)
    #pe_ttm, pe_ttm_nonrecurring = go.calculate_a_historical_date_index_PE("399997","2021-02-10")
    #print(pe_ttm, pe_ttm_nonrecurring)
    #result = go.calculate_real_time_index_pe_multiple_threads("399997")
    #print(result)
    #msg = go.calculate_all_tracking_index_funds_real_time_PE_and_generate_msg()
    #print(msg)
    #index_all_historical_pe_info = go.cal_the_real_time_PE_percentile_in_hisstory("399997.XSHE")
    #print(index_all_historical_pe_info)
    #print(index_all_historical_pe_info)
    #result = go.calculate_a_historical_date_index_PE("399997","2021-05-31")
    #print(result)
    #result = go.get_last_trading_day_PE("399997")
    #print(result)
    #result = go.cal_the_PE_percentile_in_history("399997", "sz399997")
    #print(result)
    #result = go.calculate_all_tracking_index_funds_real_time_PE_and_generate_msg()
    #print(result)
    #result = go.get_a_historical_date_index_PE('399997','2021-12-24')
    #print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)