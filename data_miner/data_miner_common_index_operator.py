import time

import sys

sys.path.append("..")
import database.db_operator as db_operator


class DataMinerCommonIndexOperator:
    # 对数据库中指数信息的通用操作

    def __init__(self):
        pass

    '''
    def get_index_constitute_stocks(self, index_code):
        # 获取数据库中的指数最新的构成股和比例
        # param: index_code 指数代码，如 399997 或者 399997.XSHE
        # 返回： 指数构成股及其权重,
        # 如 [{'global_stock_code': '000568.XSHE', 'stock_code': '000568', 'stock_name': '泸州老窖', 'weight': Decimal('14.8100')},
        # {'global_stock_code': '000596.XSHE', 'stock_code': '000596', 'stock_name': '古井贡酒', 'weight': Decimal('3.6940')}]
        selecting_sql = "SELECT global_stock_code, stock_code, stock_name, weight FROM index_constituent_stocks_weight " \
                        "WHERE index_code LIKE '%s' AND submission_date = (" \
                        "SELECT submission_date FROM index_constituent_stocks_weight " \
                        "WHERE index_code LIKE '%s' " \
                        "ORDER BY submission_date DESC LIMIT 1)" % (index_code+'%', index_code+'%')
        index_constitute_stocks_weight = db_operator.DBOperator().select_all("financial_data", selecting_sql)
        return index_constitute_stocks_weight
    '''

    def get_index_constitute_stocks(self, index_code):
        # 获取数据库中的指数最新的构成股和比例
        # param: index_code 指数代码，如 399997
        # 返回： 指数构成股及其权重,
        # 如 [{'stock_code': '000568', 'stock_name': '泸州老窖', 'weight': Decimal('15.487009751893797000'), 'stock_exchange_location': 'sz', 'stock_market_code': 'XSHE'},
        # {'stock_code': '000596', 'stock_name': '古井贡酒', 'weight': Decimal('3.438504576505159600'), 'stock_exchange_location': 'sz', 'stock_market_code': 'XSHE'},,,,]
        selecting_sql = "SELECT stock_code, stock_name, weight, stock_exchange_location, stock_market_code FROM mix_top10_with_bottom_no_repeat " \
                        "WHERE index_code = '%s' " % (index_code)
        index_constitute_stocks_weight = db_operator.DBOperator().select_all("financial_data", selecting_sql)
        return index_constitute_stocks_weight

    def get_index_name(self, index_code):
        # 根据指数代码获取指数名称
        # param: index_code 指数代码，如 399997
        # return: 指数名称, 如 中证白酒

        selecting_sql = "SELECT index_name FROM mix_top10_with_bottom_no_repeat where index_code = '%s'" % (index_code)
        index_name = db_operator.DBOperator().select_one("financial_data", selecting_sql)
        return index_name["index_name"]

    '''
    # 废弃,无用代码，tzk, 2021-12-25
        def get_today_updated_index_info(self):
        # 获取今天有更新的指数信息
        # return：今日有更新的指数代码和名称，
        # 如 {'000932.XSHG': '中证主要消费', '399997.XSHE': '中证白酒'}

        # 获取今天，指数构成有更新的指数代码及名称
        selecting_sql = "SELECT DISTINCT index_code, index_name FROM index_constituent_stocks_weight " \
                        "WHERE submission_date = date_format(now(),'%Y-%m-%d')"
        # 返回如，如 [{'index_code': '000932.XSHG', 'index_name': '中证主要消费'},
        #         #     {'index_code': '399997.XSHE', 'index_name': '中证白酒'}]
        updated_info = db_operator.DBOperator().select_all("financial_data", selecting_sql)
        # 将数据库信息简化为dict, 如 {'000932.XSHG': '中证主要消费', '399997.XSHE': '中证白酒'}
        updated_info_dict = dict()
        for info in updated_info:
            if info["index_code"] not in updated_info_dict:
                updated_info_dict[info["index_code"]] = info["index_name"]
        return updated_info_dict
    
    '''

    """
    获取指数最新某估值在过去X年的百分比
    # param: index_code 指数代码，如 399997
    # param: valuation_method 估值方式，如 pe_ttm--滚动市盈率, pe_ttm_nonrecurring--扣非滚动市盈率, pb--市净率, pb_wo_gw--扣非市净率, ps_ttm--滚动市销率, pcf_ttm--滚动市现率, dividend_yield--股息率,
    # param: years 年数，如 10
    # 返回： 
    # 如 {'index_code': '399997', 'index_name': '中证白酒', 'latest_date': datetime.date(2022, 4, 8), 'pe_ttm_effective': Decimal('46.437202040'), 'row_num': 1757, 'total_num': 2169, 'percentage': 81.0}
    """

    def get_index_latest_estimation_percentile_in_history(self, index_code, valuation_method, years):
        selecting_sql = ""
        # 滚动市盈率
        if (valuation_method == "pe_ttm"):
            selecting_sql = """  select raw.index_code, raw.index_name, raw.latest_date, round(raw.pe_ttm_effective, 4) as pe_ttm_effective, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as previous_year_num from 
                            (select index_code, index_name, historical_date as latest_date, pe_ttm*100/pe_ttm_effective_weight as pe_ttm_effective,
                            row_number() OVER (partition by index_code ORDER BY pe_ttm*100/pe_ttm_effective_weight asc) AS row_num,  
                            percent_rank() OVER (partition by index_code ORDER BY pe_ttm*100/pe_ttm_effective_weight asc) AS percent_num
                            from aggregated_data.index_components_historical_estimations
                            where index_code = '%s'
                            and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                            left join 
                            (select index_code, count(1) as total_num from aggregated_data.index_components_historical_estimations where index_code = '%s' and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
                            on raw.index_code = record.index_code
                            where raw.latest_date = (select max(historical_date) from aggregated_data.index_components_historical_estimations)
                            order by raw.percent_num asc """ % (years, index_code, years, index_code, years)
        # 扣非滚动市盈率
        elif (valuation_method == "pe_ttm_nonrecurring"):
            selecting_sql = """  select raw.index_code, raw.index_name, raw.latest_date, round(raw.pe_ttm_nonrecurring_effective, 4) as pe_ttm_nonrecurring_effective, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as previous_year_num  from 
            					(select index_code, index_name, historical_date as latest_date, pe_ttm_nonrecurring*100/pe_ttm_nonrecurring_effective_weight as pe_ttm_nonrecurring_effective,
            					row_number() OVER (partition by index_code ORDER BY pe_ttm_nonrecurring*100/pe_ttm_nonrecurring_effective_weight asc) AS row_num,  
            					percent_rank() OVER (partition by index_code ORDER BY pe_ttm_nonrecurring*100/pe_ttm_nonrecurring_effective_weight asc) AS percent_num
            					from aggregated_data.index_components_historical_estimations
            					where index_code = '%s'
            					and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
            					left join 
            					(select index_code, count(1) as total_num from aggregated_data.index_components_historical_estimations where index_code = '%s' and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
            					on raw.index_code = record.index_code
            					where raw.latest_date = (select max(historical_date) from aggregated_data.index_components_historical_estimations)
            					order by raw.percent_num asc """ % (years, index_code, years, index_code, years)
        # 市净率
        elif (valuation_method == "pb"):
            selecting_sql = """  select raw.index_code, raw.index_name, raw.latest_date, round(raw.pb_effective, 4) as pb_effective, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as previous_year_num  from 
            					(select index_code, index_name, historical_date as latest_date, pb*100/pb_effective_weight as pb_effective,
            					row_number() OVER (partition by index_code ORDER BY pb*100/pb_effective_weight asc) AS row_num,  
            					percent_rank() OVER (partition by index_code ORDER BY pb*100/pb_effective_weight asc) AS percent_num
            					from aggregated_data.index_components_historical_estimations
            					where index_code = '%s'
            					and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
            					left join 
            					(select index_code, count(1) as total_num from aggregated_data.index_components_historical_estimations where index_code = '%s' and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
            					on raw.index_code = record.index_code
            					where raw.latest_date = (select max(historical_date) from aggregated_data.index_components_historical_estimations)
            					order by raw.percent_num asc """ % (years, index_code, years, index_code, years)
        # 扣商誉市净率
        elif (valuation_method == "pb_wo_gw"):
            selecting_sql = """  select raw.index_code, raw.index_name, raw.latest_date, round(raw.pb_wo_gw_effective, 4) as pb_wo_gw_effective, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as previous_year_num  from 
            					(select index_code, index_name, historical_date as latest_date, pb_wo_gw*100/pb_wo_gw_effective_weight as pb_wo_gw_effective,
            					row_number() OVER (partition by index_code ORDER BY pb_wo_gw*100/pb_wo_gw_effective_weight asc) AS row_num,  
            					percent_rank() OVER (partition by index_code ORDER BY pb_wo_gw*100/pb_wo_gw_effective_weight asc) AS percent_num
            					from aggregated_data.index_components_historical_estimations
            					where index_code = '%s'
            					and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
            					left join 
            					(select index_code, count(1) as total_num from aggregated_data.index_components_historical_estimations where index_code = '%s' and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
            					on raw.index_code = record.index_code
            					where raw.latest_date = (select max(historical_date) from aggregated_data.index_components_historical_estimations)
            					order by raw.percent_num asc """ % (years, index_code, years, index_code, years)
        # 滚动市销率
        elif (valuation_method == "ps_ttm"):
            selecting_sql = """  select raw.index_code, raw.index_name, raw.latest_date, round(raw.ps_ttm_effective, 4) as ps_ttm_effective, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as previous_year_num  from 
            					(select index_code, index_name, historical_date as latest_date, ps_ttm*100/ps_ttm_effective_weight as ps_ttm_effective,
            					row_number() OVER (partition by index_code ORDER BY ps_ttm*100/ps_ttm_effective_weight asc) AS row_num,  
            					percent_rank() OVER (partition by index_code ORDER BY ps_ttm*100/ps_ttm_effective_weight asc) AS percent_num
            					from aggregated_data.index_components_historical_estimations
            					where index_code = '%s'
            					and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
            					left join 
            					(select index_code, count(1) as total_num from aggregated_data.index_components_historical_estimations where index_code = '%s' and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
            					on raw.index_code = record.index_code
            					where raw.latest_date = (select max(historical_date) from aggregated_data.index_components_historical_estimations)
            					order by raw.percent_num asc """ % (years, index_code, years, index_code, years)

        # 滚动市销率
        elif (valuation_method == "pcf_ttm"):
            selecting_sql = """  select raw.index_code, raw.index_name, raw.latest_date, round(raw.pcf_ttm_effective, 4) as pcf_ttm_effective, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as previous_year_num  from 
            					(select index_code, index_name, historical_date as latest_date, pcf_ttm*100/pcf_ttm_effective_weight as pcf_ttm_effective,
            					row_number() OVER (partition by index_code ORDER BY pcf_ttm*100/pcf_ttm_effective_weight asc) AS row_num,  
            					percent_rank() OVER (partition by index_code ORDER BY pcf_ttm*100/pcf_ttm_effective_weight asc) AS percent_num
            					from aggregated_data.index_components_historical_estimations
            					where index_code = '%s'
            					and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
            					left join 
            					(select index_code, count(1) as total_num from aggregated_data.index_components_historical_estimations where index_code = '%s' and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
            					on raw.index_code = record.index_code
            					where raw.latest_date = (select max(historical_date) from aggregated_data.index_components_historical_estimations)
            					order by raw.percent_num asc """ % (years, index_code, years, index_code, years)

        # 股息率
        elif (valuation_method == "dividend_yield"):
            selecting_sql = """  select raw.index_code, raw.index_name, raw.latest_date, round(raw.dividend_yield_effective, 4) as dividend_yield_effective, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as previous_year_num  from 
            					(select index_code, index_name, historical_date as latest_date, dividend_yield*100/dividend_yield_effective_weight as dividend_yield_effective,
            					row_number() OVER (partition by index_code ORDER BY dividend_yield*100/dividend_yield_effective_weight asc) AS row_num,  
            					percent_rank() OVER (partition by index_code ORDER BY dividend_yield*100/dividend_yield_effective_weight asc) AS percent_num
            					from aggregated_data.index_components_historical_estimations
            					where index_code = '%s'
            					and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
            					left join 
            					(select index_code, count(1) as total_num from aggregated_data.index_components_historical_estimations where index_code = '%s' and historical_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
            					on raw.index_code = record.index_code
            					where raw.latest_date = (select max(historical_date) from aggregated_data.index_components_historical_estimations)
            					order by raw.percent_num asc """ % (years, index_code, years, index_code, years)
        # 其它
        else:
            return None
        index_estiamtion_info = db_operator.DBOperator().select_one("aggregated_data", selecting_sql)
        return index_estiamtion_info

    # {'index_code': '000300', 'index_name': '沪深300', 'latest_date': datetime.date(2023, 5, 18), 'pe_ttm': Decimal('12.1596'), 'row_num': 527, 'total_num': 1214, 'percentage': 43.36, 'previous_year_num': '5', 'index_code_with_init': 'sh000300'}

    """
        获取沪深指数300最新某估值在过去X年的百分比
        # param: index_code 指数代码，如 000300
        # param: valuation_method 估值方式，如 pe_ttm--滚动市盈率, pb--市净率, ps_ttm--滚动市销率, dividend_yield--股息率
        # param: years 年数，如 10
        # 返回： 
        # 如 {'index_code': '000300', 'index_name': '沪深300', 'latest_date': datetime.date(2023, 5, 18), 'pe_ttm': Decimal('12.1596'), 'row_num': 527, 'total_num': 1214, 'percentage': 43.36, 'previous_year_num': '5', 'index_code_with_init': 'sh000300'}
        """
    def get_hz_three_hundred_index_latest_estimation_percentile_in_history(self, index_code, valuation_method, years):
        selecting_sql = ""
        # 滚动市盈率, 滚动市盈率市值加权，所有样品公司市值之和 / 所有样品公司归属于母公司净利润之和
        if (valuation_method == "pe_ttm"):
            selecting_sql = """select raw.index_code, raw.index_name, raw.latest_date, round(raw.pe_ttm_mcw, 4) as pe_ttm, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as                                previous_year_num, concat("sh", raw.index_code) as index_code_with_init from 
                                    (select index_code, index_name, trading_date as latest_date, pe_ttm_mcw,
                                    row_number() OVER (partition by index_code ORDER BY pe_ttm_mcw asc) AS row_num,  
                                    percent_rank() OVER (partition by index_code ORDER BY pe_ttm_mcw asc) AS percent_num
                                    from financial_data.index_estimation_from_lxr_di
                                    where index_code = '%s'
                                    and trading_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                                    left join 
                                    (select index_code, count(1) as total_num from financial_data.index_estimation_from_lxr_di where index_code = '%s' and trading_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
                                    on raw.index_code = record.index_code
                                    where raw.latest_date = (select max(trading_date) from financial_data.index_estimation_from_lxr_di)
                                    order by raw.percent_num asc """ % (years, index_code, years, index_code, years)
        # 市净率，市净率市值加权，所有样品公司市值之和 / 净资产之和
        elif (valuation_method == "pb"):
            selecting_sql = """select raw.index_code, raw.index_name, raw.latest_date, round(raw.pb_mcw, 4) as pe_ttm, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as                                previous_year_num, concat("sh", raw.index_code) as index_code_with_init from 
                                                            (select index_code, index_name, trading_date as latest_date, pb_mcw,
                                                            row_number() OVER (partition by index_code ORDER BY pb_mcw asc) AS row_num,  
                                                            percent_rank() OVER (partition by index_code ORDER BY pb_mcw asc) AS percent_num
                                                            from financial_data.index_estimation_from_lxr_di
                                                            where index_code = '%s'
                                                            and trading_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                                                            left join 
                                                            (select index_code, count(1) as total_num from financial_data.index_estimation_from_lxr_di where index_code = '%s' and trading_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
                                                            on raw.index_code = record.index_code
                                                            where raw.latest_date = (select max(trading_date) from financial_data.index_estimation_from_lxr_di)
                                                            order by raw.percent_num asc """ % (years, index_code, years, index_code, years)
        # 滚动市销率, 市销率市值加权，所有样品公司市值之和 / 营业额之和
        elif (valuation_method == "ps_ttm"):
            selecting_sql = """select raw.index_code, raw.index_name, raw.latest_date, round(raw.ps_ttm_mcw, 4) as pe_ttm, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as                                previous_year_num, concat("sh", raw.index_code) as index_code_with_init from 
                                                            (select index_code, index_name, trading_date as latest_date, ps_ttm_mcw,
                                                            row_number() OVER (partition by index_code ORDER BY ps_ttm_mcw asc) AS row_num,  
                                                            percent_rank() OVER (partition by index_code ORDER BY ps_ttm_mcw asc) AS percent_num
                                                            from financial_data.index_estimation_from_lxr_di
                                                            where index_code = '%s'
                                                            and trading_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                                                            left join 
                                                            (select index_code, count(1) as total_num from financial_data.index_estimation_from_lxr_di where index_code = '%s' and trading_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
                                                            on raw.index_code = record.index_code
                                                            where raw.latest_date = (select max(trading_date) from financial_data.index_estimation_from_lxr_di)
                                                            order by raw.percent_num asc """ % (years, index_code, years, index_code, years)

        # 股息率, 股息率市值加权，所有样品公司市值之和 / 分红之和
        elif (valuation_method == "dividend_yield"):
            selecting_sql = """select raw.index_code, raw.index_name, raw.latest_date, round(raw.dyr_mcw, 4) as pe_ttm, raw.row_num, record.total_num, round(raw.percent_num*100, 2) as percentage, '%s' as                                previous_year_num, concat("sh", raw.index_code) as index_code_with_init from 
                                                            (select index_code, index_name, trading_date as latest_date, dyr_mcw,
                                                            row_number() OVER (partition by index_code ORDER BY dyr_mcw asc) AS row_num,  
                                                            percent_rank() OVER (partition by index_code ORDER BY dyr_mcw asc) AS percent_num
                                                            from financial_data.index_estimation_from_lxr_di
                                                            where index_code = '%s'
                                                            and trading_date > SUBDATE(NOW(),INTERVAL '%s' YEAR)) raw
                                                            left join 
                                                            (select index_code, count(1) as total_num from financial_data.index_estimation_from_lxr_di where index_code = '%s' and trading_date > SUBDATE(NOW(),INTERVAL '%s' YEAR) group by index_code) as record
                                                            on raw.index_code = record.index_code
                                                            where raw.latest_date = (select max(trading_date) from financial_data.index_estimation_from_lxr_di)
                                                            order by raw.percent_num asc """ % (years, index_code, years, index_code, years)

        # 其它
        else:
            return None
        index_estiamtion_info = db_operator.DBOperator().select_one("financial_data", selecting_sql)
        return index_estiamtion_info


if __name__ == '__main__':
    time_start = time.time()
    go = DataMinerCommonIndexOperator()
    # index_constitute_stocks_weight = go.get_index_constitute_stocks('399997')
    # print(index_constitute_stocks_weight)
    # index_name = go.get_index_name("399997")
    # print(index_name)
    # result = go.get_today_updated_index_info()
    # print(result)
    #result = go.get_index_latest_estimation_percentile_in_history("399986", "pb_wo_gw", 5)
    result = go.get_hz_three_hundred_index_latest_estimation_percentile_in_history("000300", "pe_ttm", 8)
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)
