import time
import sys

sys.path.append("..")
import database.db_operator as db_operator

class DataMinerCommonTargetIndexOperator:
    # 读取标的池中关于指数的信息

    def __init__(self):
        pass

    '''
    # 可用代码，暂时未被引用，2022.04.05
        def get_indexes_and_their_names(self):
        # 获取标的池中跟踪关注指数及他们的中文名称
        # 输入：无
        # 输出：获取标的池中跟踪关注指数的指数代码，中文名称, 地点缩写+指数代码，指数代码+证券市场代码
        # 如，[{'index_code': '399997', 'index_name': '中证白酒指数', 'index_code_with_init': 'sz399997', 'index_code_with_market_code': '399997.XSHE'},,,]

        # 查询SQL
        selecting_sql = "select index_code, index_name, concat(exchange_location_1,index_code) as index_code_with_init, " \
                        "concat(index_code,'.',exchange_location_2) as index_code_with_market_code from index_target"

        # 查询
        selecting_result = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        # 返回 如
        # [{'index_code': '399997', 'index_name': '中证白酒指数', 'index_code_with_init': 'sz399997', 'index_code_with_market_code': '399997.XSHE'},,,,]
        return selecting_result
    '''


    '''
        # 可用代码，暂时未被引用，2022.04.05
        def get_index_valuation_method(self):
        # 获取标的池中跟踪关注指数的估值方式
        # 输入：无
        # 输出：获取标的池中跟踪关注指数的指数代码，中文名称, 地点缩写+指数代码，指数代码+证券市场代码，估值方式
        # 如 [{'index_code': '399997', 'index_name': '中证白酒指数', 'index_code_with_init': 'sz399997', 'index_code_with_market_code': '399997.XSHE', 'valuation_method': 'pe'},，，，]

        # 查询SQL
        selecting_sql = "select index_code, index_name, concat(exchange_location_1,index_code) as index_code_with_init, " \
                        "concat(index_code,'.',exchange_location_2) as index_code_with_market_code, valuation_method from index_target"

        # 查询
        selecting_result = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        # 返回 如
        # [{'index_code': '399997', 'index_name': '中证白酒指数', 'index_code_with_init': 'sz399997', 'index_code_with_market_code': '399997.XSHE', 'valuation_method': 'pe'},，，，]
        return selecting_result
    '''


    def index_valuated_by_method(self, method):
        # 获取通过xx估值法 估值的指数代码及其对应名称
        # 输入：method, 估值方式，目前有 pe_ttm：市盈率估值法； pb:市净率估值法
        # 输出：使用该估值方式的指数代码，中文名称, 地点缩写+指数代码，指数代码+证券市场代码
        # 如 [{'index_code': '399965', 'index_name': '中证800地产', 'index_code_with_init': 'sz399965', 'index_code_with_market_code': '399965.XSHE'},，，]

        # 查询SQL
        selecting_sql = "select target_code as index_code, target_name as index_name, concat(exchange_location,target_code) as index_code_with_init, concat(target_code,'.',exchange_location_mic) as index_code_with_market_code from investment_target where target_type = 'index' and status = 'active' and trade='buy' and valuation_method = '%s'  " % (method)

        # 查询
        selecting_result = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        # 返回 如
        # [{'index_code': '399965', 'index_name': '中证800地产', 'index_code_with_init': 'sz399965', 'index_code_with_market_code': '399965.XSHE'},,,]
        return selecting_result

    def get_given_index_company_index(self,company_name):
        # 获取特定指数公司开发的指数，指数代码及指数名称
        # 输入：company_name，指数公司名称
        # 输出：
        # 如 查询 "中证"
        # # 如 [{'index_code': '399965', 'index_name': '中证800地产', 'index_code_with_init': 'sz399965', 'index_code_with_market_code': '399965.XSHE'},，，]

        # 查询SQL
        selecting_sql = "select target_code as index_code, target_name as index_name, concat(exchange_location,target_code) as index_code_with_init, concat(target_code,'.',exchange_location_mic) as index_code_with_market_code from investment_target where target_type = 'index' and status = 'active' and trade='buy' and index_company = '%s'  " % (company_name)

        # 查询
        selecting_result = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        # 返回 如
        # [{'index_code': '399965', 'index_name': '中证800地产', 'index_code_with_init': 'sz399965', 'index_code_with_market_code': '399965.XSHE'},,,]
        return selecting_result

    '''
        # 可用代码，暂时未被引用，2022.04.05
        def get_indexes_names_companies(self):
        # 获取标的池中跟踪关注指数,他们的中文名称及指数开发公司
        # 输入：无
        # 输出：获取标的池中跟踪关注指数,他们的中文名称及指数开发公司。
        # 如 [{'index_code': '399997', 'index_name': '中证白酒指数', 'index_code_with_init': 'sz399997', 'index_code_with_market_code': '399997.XSHE', 'index_company': '中证'},,,,]

        # 查询SQL
        selecting_sql = "select index_code, index_name, concat(exchange_location_1,index_code) as index_code_with_init, " \
                        "concat(index_code,'.',exchange_location_2) as index_code_with_market_code, index_company from index_target"

        # 查询
        selecting_result = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        # 返回 如
        # [{'index_code': '399997', 'index_name': '中证白酒指数', 'index_code_with_init': 'sz399997', 'index_code_with_market_code': '399997.XSHE', 'index_company': '中证'},,,,]
        return selecting_result
    '''

    def get_given_index_trigger_info(self,target_code, method):
        # 获取特定指数，在一定方法下的 策略触发信息
        # 输入：target_code，指数代码
        # 输入：method, 估值方式，目前有 pe_ttm：市盈率估值法； pb:市净率估值法；equity_bond_yield：股债收益率；

        # 输出：
        # 如 查询 "diy_000300-cn10yr" + "equity_bond_yield"
        # # 如 [{'index_code': '399965', 'index_name': '中证800地产', 'index_code_with_init': 'sz399965', 'index_code_with_market_code': '399965.XSHE'},，，]

        # 查询SQL
        selecting_sql = "select trigger_value, trigger_percent " \
                        "from investment_target where target_type = 'index' and status = 'active' and trade='buy' " \
                        "and target_code = '%s'  and valuation_method = '%s' " % (target_code, method)

        # 查询
        selecting_result = db_operator.DBOperator().select_one("target_pool", selecting_sql)
        # 返回 如
        # [{'index_code': '399965', 'index_name': '中证800地产', 'index_code_with_init': 'sz399965', 'index_code_with_market_code': '399965.XSHE'},,,]
        return selecting_result


if __name__ == '__main__':
    time_start = time.time()
    go = DataMinerCommonTargetIndexOperator()
    #result = go.get_given_index_company_index('中证')
    result = go.get_given_index_trigger_info("diy_000300-cn10yr","equity_bond_yield")
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)