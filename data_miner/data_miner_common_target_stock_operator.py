import time
import sys

sys.path.append("..")
import database.db_operator as db_operator


class DataMinerCommonTargetStockOperator:
    # 读取标的池中关于股票的信息

    def __init__(self):
        pass

    ''' 
        # 可用代码，暂时未被引用，2022.04.05
        def get_stocks_and_their_names(self):
        # 获取标的池中跟踪关注股票及他们的中文名称
        # 输入：无
        # 输出：股票代码，股票名称， 地点缩写+指数代码，指数代码+证券市场代码
        # 如 [{'stock_code': '000002', 'stock_name': '万科A', 'stock_code_with_init': 'sz000002', 'stock_code_with_market_code': '000002.XSHE'}, ,,,]

        # 查询SQL
        selecting_sql = "select stock_code, stock_name, concat(exchange_location_1,stock_code) as stock_code_with_init, " \
                        "concat(stock_code,'.',exchange_location_2) as stock_code_with_market_code from stock_target"

        # 查询
        selecting_result = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        # 返回 如
        # [{'stock_code': '000002', 'stock_name': '万科A', 'stock_code_with_init': 'sz000002', 'stock_code_with_market_code': '000002.XSHE'}, ,,,]
        return selecting_result
    '''



    def get_stocks_valuation_method_and_trigger(self):
        # 获取标的池中跟踪关注股票及对应的估值方式和触发条件
        # 输入：无
        # 输出：股票代码，股票名称， 地点缩写+指数代码，指数代码+证券市场代码, 估值方式，触发值，触发历史百分位

        # 查询SQL
        selecting_sql = "select target_code as stock_code, target_name as stock_name, concat(exchange_location,target_code) as stock_code_with_init, " \
                        "concat(target_code,'.',exchange_location_mic) as stock_code_with_market_code, valuation_method, " \
                        "trigger_value, trigger_percent from investment_target where target_type = 'stock' and status = 'active'  and trade='buy' "

        # 查询
        selecting_result = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        # 返回 如
        # [{'stock_code': '000002', 'stock_name': '万科A', 'stock_code_with_init': 'sz000002', 'stock_code_with_market_code': '000002.XSHE', 'valuation_method': 'pb', 'trigger_value': Decimal('0.95'), 'trigger_percent': Decimal('0.50')},,,,]
        return selecting_result


    ''' 
        # 可用代码，暂时未被引用，2022.04.05
        def get_stocks_monitoring_frequency(self):
        # 获取标的池中跟踪关注股票及对应的监控频率策略
        # 输入：无
        # 输出：股票代码，股票名称， 地点缩写+指数代码，指数代码+证券市场代码, 监控频率策略

        # 查询SQL
        selecting_sql = "select stock_code, stock_name, concat(exchange_location_1,stock_code) as stock_code_with_init, " \
                        "concat(stock_code,'.',exchange_location_2) as stock_code_with_market_code, monitoring_frequency " \
                        "from stock_target"

        # 查询
        selecting_result = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        # 返回 如
        # [{'stock_code': '000002', 'stock_name': '万科A', 'stock_code_with_init': 'sz000002', 'stock_code_with_market_code': '000002.XSHE', 'monitoring_frequency': 'minutely'},,,,]
        return selecting_result
    '''


if __name__ == '__main__':
    time_start = time.time()
    go = DataMinerCommonTargetStockOperator()
    result = go.get_stocks_valuation_method_and_trigger()
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)