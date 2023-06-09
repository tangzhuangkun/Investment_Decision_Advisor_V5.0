#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import datetime
import json
import sys
import time
from datetime import date
from dateutil.relativedelta import relativedelta
import requests

sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger
import data_miner.data_miner_common_db_operator as data_miner_common_db_operator
import db_mapper.parser_component.token_record_mapper as token_record_mapper



class CollectStockHistoricalEstimationInfo:
    # 收集所需的股票的估值信息
    # PE-TTM :pe_ttm
    # PE-TTM(扣非) :d_pe_ttm
    # PB :pb
    # PS-TTM :ps_ttm
    # PCF-TTM :pcf_ttm
    # EV/EBIT :ev_ebit_r
    # 股票收益率 :ey
    # 股息率 :dyr
    # 股价 :sp
    # 成交量 :tv
    # 前复权 :fc_rights
    # 后复权 :bc_rights
    # 理杏仁前复权 :lxr_fc_rights
    # 股东人数 :shn
    # 市值 :mc
    # 流通市值 :cmc
    # 自由流通市值 :ecmc
    # 人均自由流通市值 :ecmc_psh
    # 融资余额 :fb
    # 融券余额 :sb
    # 陆股通持仓金额 :ha_shm

    # 运行频率：每天收盘后


    def __init__(self):
        # 从数据库取数时，每页取的条数信息
        # 每次向杏理仁请求数据时，每次申请的条数
        self.page_size = 80
        # 获取当前日期
        self.today = time.strftime("%Y-%m-%d", time.localtime())
        # 收集数据的起始日期
        self.estimation_start_date = "2010-01-01"

    def get_all_exchange_locaiton_mics(self):
        '''
        获取所有交易所的mic码
        :return: mic码的list，如 ['XSHE', 'XHKG', 'XSHG']
        '''
        selecting_sql = "SELECT DISTINCT exchange_location_mic FROM all_tracking_stocks_rf"
        all_exchange_locaiton_mics_dict = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        exchange_location_mic_list = list()
        for mic in all_exchange_locaiton_mics_dict:
            exchange_location_mic_list.append(mic.get("exchange_location_mic"))
        return exchange_location_mic_list

    def all_tracking_stocks(self, exchange_location_mic):
        '''
        数据库中，某交易所，全部需要被收集估值信息的股票
        :param exchange_location_mic: 交易所MIC码（如 XSHG, XSHE，XHKG）均可， 大小写均可
        :return:
        '''
        # 数据库中，全部需要被收集估值信息的股票
        # 输出： 需要被收集估值信息的股票代码，股票名称，上市地代码的字典
        # 如 [{'stock_code': '002688', 'stock_name': '金河生物', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
        #     {'stock_code': '603696', 'stock_name': '安记食品', 'exchange_location': 'sh', 'exchange_location_mic': 'XSHG'},
        #     {'stock_code': '002234', 'stock_name': '民和股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
        #     {'stock_code': '00700', 'stock_name': '腾讯控股', 'exchange_location': 'hk', 'exchange_location_mic': 'XHKG'},,, , , ]

        # 统一处理为大写
        exchange_location_mic = exchange_location_mic.upper()

        selecting_sql = """SELECT DISTINCT stock_code, stock_name, exchange_location, exchange_location_mic 
                            FROM all_tracking_stocks_rf where exchange_location_mic = '%s' """ % (exchange_location_mic)
        all_tracking_stock_dict = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        # stock_codes_names_dict 如  [{'stock_code': '002688', 'stock_name': '金河生物', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
        # {'stock_code': '603696', 'stock_name': '安记食品', 'exchange_location': 'sh', 'exchange_location_mic': 'XSHG'},
        # {'stock_code': '002234', 'stock_name': '民和股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
        # {'stock_code': '00700', 'stock_name': '腾讯控股', 'exchange_location': 'hk', 'exchange_location_mic': 'XHKG'},,, , , ]
        return all_tracking_stock_dict

    def all_tracking_stocks_counter(self,exchange_location_mic):
        '''
        数据库中，某交易所，全部需要被收集估值信息的股票的统计数
         :param exchange_location_mic: 交易所MIC码（如 XSHG, XSHE，XHKG）均可， 大小写均可
        :return: 统计数，如 108
        '''
        # 统一处理为大写
        exchange_location_mic = exchange_location_mic.upper()

        selecting_sql = """SELECT count(DISTINCT stock_code) as counter FROM all_tracking_stocks_rf 
                            where exchange_location_mic = '%s' """ % (exchange_location_mic)
        stock_codes_counter = db_operator.DBOperator().select_one("target_pool", selecting_sql)
        return stock_codes_counter['counter']


    def the_stocks_that_already_in_db(self, exchange_location_mic, latest_collection_date):
        '''
        某个交易所，获取数据库中已有的且也是那些需要被跟踪的股票
        :param exchange_location_mic: 交易所MIC码（如 XSHG, XSHE，XHKG）均可， 大小写均可
        :param latest_collection_date: stocks_main_estimation_indexes_historical_data收集数据的最新日期， 如 2022-04-03
        :return:
        如
        [{'stock_code': '000858', 'stock_name': '五粮液', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
        {'stock_code': '002714', 'stock_name': '牧原股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},,，，，]
        '''
        # 统一处理为大写
        exchange_location_mic = exchange_location_mic.upper()
        selecting_sql = """ 
                            select tar.stock_code, tar.stock_name, tar.exchange_location, tar.exchange_location_mic from 
                            -- 某个交易所，需要跟踪的全部股票
                            (select distinct stock_code, stock_name, exchange_location, exchange_location_mic 
                                        from target_pool.all_tracking_stocks_rf
                                        where exchange_location_mic = '%s' ) tar
                            inner join 
                            -- 数据库中已有的某个交易所的全部股票
                            (select distinct stock_code, date, exchange_location_mic
                            from financial_data.stocks_main_estimation_indexes_historical_data
                            where date = '%s'
                            and exchange_location_mic = '%s' ) his
                            on his.stock_code = tar.stock_code
                            and his.exchange_location_mic = tar.exchange_location_mic """ % (exchange_location_mic, latest_collection_date, exchange_location_mic)
        saved_stock_info_list = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        return saved_stock_info_list

    def the_stocks_that_already_in_db_counter(self, exchange_location_mic, latest_collection_date):
        '''
        某个交易所，获取数据库中已有的, 且也是那些需要被跟踪的股票的个数
        :param exchange_location_mic: 交易所MIC码（如 XSHG, XSHE，XHKG）均可， 大小写均可
        :param latest_collection_date: stocks_main_estimation_indexes_historical_data收集数据的最新日期， 如 2022-04-03
        :return: 统计数， 如 66
        '''

        saved_stocks_info_counter = len(self.the_stocks_that_already_in_db(exchange_location_mic, latest_collection_date))
        return saved_stocks_info_counter


    def the_stocks_that_not_in_db(self, exchange_location_mic, latest_collection_date):
        '''
        某个交易所，需要被跟踪,但暂时不在数据库中的股票
        :param exchange_location_mic: 交易所MIC码（如 XSHG, XSHE，XHKG）均可， 大小写均可
        :param latest_collection_date: stocks_main_estimation_indexes_historical_data收集数据的最新日期， 如 2022-04-03
        :return:
        [{'stock_code': '000858', 'stock_name': '五粮液', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
        {'stock_code': '300146', 'stock_name': '汤臣倍健', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},，，，]
        '''
        # 统一处理为大写
        exchange_location_mic = exchange_location_mic.upper()
        selecting_sql = """ select tar.stock_code, tar.stock_name, tar.exchange_location, tar.exchange_location_mic from 
                            -- 某个交易所，需要跟踪的全部股票
                            (select distinct stock_code, stock_name, exchange_location, exchange_location_mic 
                                                    from target_pool.all_tracking_stocks_rf
                                                    where exchange_location_mic = '%s' ) tar
                            left join 
                            -- 数据库中已有的某个交易所的全部股票
                            (select distinct stock_code, date, exchange_location_mic
                            from financial_data.stocks_main_estimation_indexes_historical_data
                            where date = '%s'
                            and exchange_location_mic = '%s') his
                            on his.stock_code = tar.stock_code
                            and his.exchange_location_mic = tar.exchange_location_mic
                            where his.stock_code is null""" % (exchange_location_mic, latest_collection_date, exchange_location_mic)
        not_saved_stock_info_list = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        return not_saved_stock_info_list


    def the_stocks_that_not_in_db_counter(self, exchange_location_mic, latest_collection_date):
        '''
        某个交易所，需要被跟踪,但暂时不在数据库中的股票个数
        :param exchange_location_mic: 交易所MIC码（如 XSHG, XSHE，XHKG）均可， 大小写均可
        :param latest_collection_date: stocks_main_estimation_indexes_historical_data收集数据的最新日期， 如 2022-04-03
        :return: 统计数， 如 1
        '''
        not_saved_stocks_info_counter  = len(self.the_stocks_that_not_in_db(exchange_location_mic, latest_collection_date))
        return not_saved_stocks_info_counter

    def paged_demanded_stocks(self, exchange_location_mic, page, size):
        '''
        根据分页信息获取,某个交易所，哪些股票需要被收集估值信息
        :param exchange_location_mic: 交易所MIC码（如 XSHG, XSHE，XHKG）均可， 大小写均可
        :param page: 分页的页码
        :param size: 每页条数
        :return: 返回该页的 股票代码，股票名称，上市地代码 字典
        # 如：{{'000001': {'stock_code': '000001', 'stock_name': '平安银行', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
        # '000002': {'stock_code': '000002', 'stock_name': '万科A', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
        # '000031': {'stock_code': '000031', 'stock_name': '大悦城', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
        # ,,,,}
        '''

        # 统一处理为大写
        exchange_location_mic = exchange_location_mic.upper()

        start_row = page*size
        selecting_sql = """SELECT DISTINCT stock_code, stock_name, exchange_location, exchange_location_mic 
                            FROM all_tracking_stocks_rf where exchange_location_mic = '%s' 
                            order by stock_code limit %d,%d """ % (exchange_location_mic,start_row,size)
        paged_stock_info = db_operator.DBOperator().select_all("target_pool", selecting_sql)
        paged_stock_code_info_dict = dict()
        for piece in paged_stock_info:
            paged_stock_code_info_dict[piece["stock_code"]] = piece
        return paged_stock_code_info_dict

    def page_counter_by_page_size_per_page(self,exchange_location_mic):
        '''
        按X个为一页，将总的股票个数分成多页
        :param exchange_location_mic: 交易所MIC码（如 XSHG, XSHE，XHKG）均可， 大小写均可
        :return: 需要被分成多少页, 如：3
        '''

        # 统一处理为大写
        exchange_location_mic = exchange_location_mic.upper()
        stock_codes_total_counter = self.all_tracking_stocks_counter(exchange_location_mic)

        # 如果分页后，有余数（即未满一页的）
        if stock_codes_total_counter%self.page_size > 0:
            # 总页数需要加1
            page_counter = (stock_codes_total_counter//self.page_size) +1
        # 如果分页后，刚好分完，无余数
        else:
            page_counter = stock_codes_total_counter // self.page_size
        return page_counter

    def get_stock_code_name(self, stock_info_dicts):
        '''
        获取 股票代码和名称的对应关系的简洁字典
        :param stock_info_dicts: stock_info_dicts 股票代码,名称,上市地 字典, 1/多支股票，
        #         如  {{'000001': {'stock_code': '000001', 'stock_name': '平安银行', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
        #         # '000002': {'stock_code': '000002', 'stock_name': '万科A', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},,,,}
        :return: 返回如， {'000001': '平安银行', '000002': '万科A', '000031': '大悦城'}
        '''
        stock_code_name_dict = dict()
        for stock_code in stock_info_dicts:
            stock_code_name_dict[stock_code] = stock_info_dicts.get(stock_code).get('stock_name')
        return stock_code_name_dict

    def tell_exchange_market_and_determine_url(self, exchange_location_mic):
        '''
        # 判断股票属于哪个交易市场，并决定使用哪个基本面数据URL
        :param exchange_location_mic: 交易所MIC码（如 XSHG, XSHE，XHKG）均可， 大小写均可
        :return: 使用理杏仁哪个URL来获取数据
        '''

        # 上市地代码或mic码，转为大写
        exchange_location_mic = exchange_location_mic.upper()

        # 如果为 上海，上交所，深圳，深交所
        if(exchange_location_mic=='XSHG' or exchange_location_mic=='XSHE'):
            return 'https://open.lixinger.com/api/cn/company/fundamental/non_financial'
        # 如果为 香港，港交所
        elif (exchange_location_mic=='XHKG'):
            return 'https://open.lixinger.com/api/hk/company/fundamental/non_financial'
        else:
            # 日志记录失败
            msg = exchange_location_mic + ' 该交易所MIC码不可用'
            custom_logger.CustomLogger().log_writter(msg, 'error')
            return 'Unavailable Market Code'

    def tell_exchange_market_and_determine_what_estimations_need_to_get(self, exchange_location_mic):
        '''
        # 判断股票属于哪个交易市场，并决定需要获取哪些指标
        :param exchange_location_mic: 交易所MIC码（如 XSHG, XSHE，XHKG）均可， 大小写均可
        :return:
        # 接口参数，PE-TTM :pe_ttm
        # PE-TTM(扣非) :d_pe_ttm
        # PB :pb
        # PS-TTM :ps_ttm
        # PCF-TTM :pcf_ttm
        # EV/EBIT :ev_ebit_r
        # 股票收益率 :ey
        # 股息率 :dyr
        # 股价 :sp
        # 成交量 :tv
        # 前复权 :fc_rights
        # 后复权 :bc_rights
        # 理杏仁前复权 :lxr_fc_rights
        # 股东人数 :shn
        # 市值 :mc
        # 流通市值 :cmc
        # 自由流通市值 :ecmc
        # 人均自由流通市值 :ecmc_psh
        # 融资余额 :fb
        # 融券余额 :sb
        # 陆股通持仓金额 :ha_shm

        # 如果为 上海，上交所，深圳，深交所，
        ["pe_ttm", "d_pe_ttm", "pb", "pb_wo_gw", "ps_ttm", "pcf_ttm", "ev_ebit_r", "ey", "dyr", "sp", "tv",
        "fc_rights", "bc_rights", "lxr_fc_rights", "shn", "mc", "cmc", "ecmc", "ecmc_psh", "fb", "sb", "ha_shm"]

        # 如果为 香港，港交所
        ["pe_ttm", "pb", "ps_ttm", "pcf_ttm", "dyr", "sp", "tv", "fc_rights", "bc_rights", "lxr_fc_rights", "shn", "mc"]
        '''
        # 上市地代码或mic码，转为大写
        exchange_location_mic = exchange_location_mic.upper()
        # 如果为上交所，深交所
        if (exchange_location_mic == 'XSHG' or exchange_location_mic == 'XSHE'):
            return ["pe_ttm", "d_pe_ttm", "pb", "pb_wo_gw", "ps_ttm", "pcf_ttm", "ev_ebit_r", "ey", "dyr", "sp", "tv",
                     "fc_rights", "bc_rights", "lxr_fc_rights", "shn", "mc", "cmc", "ecmc", "ecmc_psh", "fb", "sb",
                     "ha_shm"]
        # 如果为 港交所
        elif (exchange_location_mic == 'XHKG'):
            return ["pe_ttm", "pb", "ps_ttm", "pcf_ttm", "dyr", "sp", "tv", "fc_rights", "bc_rights", "lxr_fc_rights", "mc"]
        else:
            # 日志记录失败
            msg = exchange_location_mic + ' 该上市地代码不可用'
            custom_logger.CustomLogger().log_writter(msg, 'error')
            return 'Unavailable Market Code'

    '''
    获取需要收集数据的起止日期list
    当需要收集的两个日期超过10年时，将日期按9年进行分割
    param: start_date， 收集数据的起始日期，必选， 如 2010-01-01
    param: end_date， 收集数据的截止日期， 必选， 如 2032-01-05
    return:
        一个日期list，
        例如 ['2010-01-01', '2019-01-01', '2019-01-02', '2028-01-02', '2028-01-03', '2032-01-05']
    '''
    def collect_data_date_when_period_longger_than_ten_years_list(self, start_date, end_date):
        # start_date = '2010-01-01'
        # end_date = '2020-08-01'
        # 用于保存需要收集的日期list
        date_list = []
        # 将日期str转化为datetime
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        # 两个日期间隔的年数
        years_diff = relativedelta(dt1=end_date, dt2=start_date).years
        # 如果间隔少于10年
        if (years_diff < 10):
            # 直接收集两个起止日期即可
            date_list.append(str(start_date))
            date_list.append(str(end_date))
            return date_list
        # 如果间隔多于10年，需要按9年为间隔进行分割
        else:
            while (start_date < end_date):
                # 起始日期加9年
                nine_more_years = start_date + relativedelta(years=9)
                # 起始日期加9年又1天
                nine_more_years_add_one_day = nine_more_years + relativedelta(days=1)
                # 如果9年又1天后仍小于截止日期
                if (nine_more_years_add_one_day < end_date):
                    date_list.append(str(start_date))
                    date_list.append(str(nine_more_years))
                    # 起始日期变为 9年又1天后
                    start_date = nine_more_years_add_one_day
                # 如果9年又1天后大于截止日期
                else:
                    date_list.append(str(start_date))
                    date_list.append(str(end_date))
                    return date_list

    def collect_a_period_time_estimation(self, stock_code, stock_info_dict, start_date, end_date, exchange_location_mic):
        # 调取理杏仁接口，获取单个股票一段时间范围内，该股票估值数据, 并储存
        # param: stock_code, 股票代码, 如 000001
        # param:  stock_info_dict 股票代码,名称,上市地 字典, 只能1支股票，
        #         如  {'000001': {'stock_code': '000001', 'stock_name': '平安银行', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}}
        # param:  start_date, 开始时间，如 2020-11-12
        # param:  end_date, 结束时间，默认为空，如 2020-11-13
        # :param exchange_location_mic: 交易所MIC码（如XSHG, XSHE，XHKG）均可， 大小写均可
        # 输出： 将获取到股票估值数据存入数据库

        # 上市地代码或mic码，转为大写
        exchange_location_mic = exchange_location_mic.upper()
        # 判断股票属于哪个交易市场，并决定使用理杏仁哪个获取基本面数据URL
        # 理杏仁 获取基本面数据 接口
        url = self.tell_exchange_market_and_determine_url(exchange_location_mic)
        # 判断股票属于哪个交易市场，并决定需要获取哪些指标
        estimations_list = self.tell_exchange_market_and_determine_what_estimations_need_to_get(exchange_location_mic)

        # 随机获取一个token
        token = token_record_mapper.TokenRecordMapper().get_one_token("lxr")
        # 理杏仁要求 在请求的headers里面设置Content-Type为application/json。
        headers = {'Content-Type': 'application/json'}

        # 获取需要收集数据的起止日期list
        # 当需要收集的两个日期超过10年时，将日期按9年进行分割
        date_list = self.collect_data_date_when_period_longger_than_ten_years_list(start_date, end_date)
        # 遍历需要收集数据的起止日期list
        for i in range(0, len(date_list), 2):
            # 收集起始时间
            colleting_start_date = date_list[i]
            # 收集结束时间
            colleting_end_date = date_list[i+1]

            parms = {"token": token,
                     "startDate": colleting_start_date,
                     "endDate": colleting_end_date,
                     "stockCodes":
                         [stock_code],
                     "metricsList": estimations_list}
            # 日志记录
            #msg = '向理杏仁请求的接口参数 ' + str(parms)
            #custom_logger.CustomLogger().log_writter(msg, 'info')
            values = json.dumps(parms)
            # 调用理杏仁接口
            req = requests.post(url, data=values, headers=headers)
            content = req.json()

            # 日志记录
            #msg = '理杏仁接口返回 ' + str(content)
            #custom_logger.CustomLogger().log_writter(msg, 'info')

            # content 如 {'code': 0, 'message': 'success', 'data': [{'date': '2020-11-13T00:00:00+08:00', 'pe_ttm': 48.04573277785343, 'd_pe_ttm': 47.83511443886097, 'pb': 14.42765025023379, 'pb_wo_gw': 14.42765025023379, 'ps_ttm': 22.564315310000808, 'pcf_ttm': 49.80250701327664, 'ev_ebit_r': 33.88432187818624, 'ey': 0.029323867066169462, 'dyr': 0.00998533724340176, 'sp': 1705, 'tv': 2815500, 'shn': 114300, 'mc': 2141817249000, 'cmc': 2141817249000, 'ecmc': 899670265725, 'ecmc_psh': 7871131, 'ha_shm': 173164289265, 'fb': 17366179363, 'sb': 2329851810, 'fc_rights': 1705, 'bc_rights': 1705, 'lxr_fc_rights': 1705, 'stockCode': '600519'}, {'date': '2020-11-12T00:00:00+08:00', 'pe_ttm': 48.88519458398379, 'd_pe_ttm': 48.67089629172529, 'pb': 14.679732186277464, 'pb_wo_gw': 14.679732186277464, 'ps_ttm': 22.95856220330575, 'pcf_ttm': 50.672663426136175, 'ev_ebit_r': 34.47543387050795, 'ey': 0.028824017925678805, 'dyr': 0.009813867960963575, 'sp': 1734.79, 'tv': 2347300, 'shn': 114300, 'mc': 2179239381462, 'cmc': 2179239381462, 'ecmc': 915389431248, 'ecmc_psh': 8008656, 'ha_shm': 176618263840, 'fb': 17207679699, 'sb': 2426239128, 'fc_rights': 1734.79, 'bc_rights': 1734.79, 'lxr_fc_rights': 1734.79, 'stockCode': '600519'}]}


            if 'error' in content and content.get('error').get('message') == "Illegal token.":
                # 日志记录失败
                msg = '使用无效TOKEN ' + token + ' ' + '执行函数collect_a_period_time_estimation获取股票代码 ' + \
                      stock_code + ' ' +str(stock_info_dict.get(stock_code).get('stock_name')) + ' ' + colleting_start_date + ' ' + colleting_end_date + ' 失败'
                custom_logger.CustomLogger().log_writter(msg, 'error')
                return self.collect_a_period_time_estimation(stock_code, stock_info_dict, colleting_start_date, colleting_end_date, exchange_location_mic)

            try:
                # 数据存入数据库
                self.save_content_into_db(content, stock_info_dict, "period")
                # 日志记录
                msg = "收集了 "+ exchange_location_mic +"."+stock_code + " 从" + colleting_start_date+" 至 "+colleting_end_date +" 的估值数据"
                custom_logger.CustomLogger().log_writter(msg, 'info')
            except Exception as e:
                # 日志记录失败
                msg = '数据存入数据库失败。 ' + '理杏仁接口返回为 '+str(content) + '。 抛错为 '+ str(e) + ' 使用的Token为' + token
                custom_logger.CustomLogger().log_writter(msg, 'error')


    def collect_a_special_date_estimation(self, stock_info_dicts, date, exchange_location_mic):
        # 调取理杏仁接口，获取特定一天，一只/多支股票估值数据, 并储存
        # param:  stock_info_dicts 股票代码,名称,上市地 字典, 1/多支股票，
        #         如  {{'000001': {'stock_code': '000001', 'stock_name': '平安银行', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
        #         # '000002': {'stock_code': '000002', 'stock_name': '万科A', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},,,,}
        # param:  date, 日期，如 2020-11-12
        # :param exchange_location_mic: 交易所MIC码（如XSHG, XSHE，XHKG）均可， 大小写均可
        # 输出： 将获取到股票估值数据存入数据库

        # 上市地代码或mic码，转为大写
        exchange_location_mic = exchange_location_mic.upper()
        # 判断股票属于哪个交易市场，并决定使用理杏仁哪个获取基本面数据URL
        # 理杏仁 获取基本面数据 接口
        url = self.tell_exchange_market_and_determine_url(exchange_location_mic)
        # 判断股票属于哪个交易市场，并决定需要获取哪些指标
        estimations_list = self.tell_exchange_market_and_determine_what_estimations_need_to_get(exchange_location_mic)

        # 随机获取一个token
        token = token_record_mapper.TokenRecordMapper().get_one_token("lxr")
        #token = "44589fdc-423d-4a31-b82d-83f94d626661"
        # 理杏仁要求 在请求的headers里面设置Content-Type为application/json。
        headers = {'Content-Type': 'application/json'}
        parms = {"token": token,
                 "date": str(date),
                 "stockCodes":
                     list(stock_info_dicts.keys()),
                 "metricsList": estimations_list}
        # 日志记录
        #msg = '向理杏仁请求的接口参数 ' + str(parms)
        #custom_logger.CustomLogger().log_writter(msg, 'info')
        values = json.dumps(parms)
        req = requests.post(url, data=values, headers=headers)
        content = req.json()
        # 日志记录
        #msg = '理杏仁接口返回 ' + str(content)
        #custom_logger.CustomLogger().log_writter(msg, 'info')

        # content 如 {'code': 0, 'message': 'success', 'data': [
        # {'date': '2020-11-13T00:00:00+08:00', 'pe_ttm': 48.2957825939533, 'd_pe_ttm': 48.62280236330014, 'pb': 12.491374104141297, 'pb_wo_gw': 12.491374104141297, 'ps_ttm': 17.156305422424143, 'pcf_ttm': 63.68585789882434, 'ev_ebit_r': 37.63760421342357, 'ey': 0.026088654822125818, 'dyr': 0.0085167925205312, 'sp': 186.69, 'tv': 13479800, 'shn': 113900, 'mc': 273454640491.19998, 'cmc': 273371391686, 'ecmc': 133902847844, 'ecmc_psh': 1175618, 'ha_shm': 5559655830, 'fc_rights': 186.69, 'bc_rights': 186.69, 'lxr_fc_rights': 186.69, 'stockCode': '000568'}, {'date': '2020-11-13T00:00:00+08:00', 'pe_ttm': 60.72940789130697, 'd_pe_ttm': 64.53258136924804, 'pb': 11.823197225442007, 'pb_wo_gw': 12.43465627328832, 'ps_ttm': 11.182674688897702, 'pcf_ttm': 216.59317739098543, 'ev_ebit_r': 48.032852603259784, 'ey': 0.02045252811485187, 'dyr': 0.006568863586599518, 'sp': 228.35, 'tv': 3310800, 'shn': 31900, 'mc': 114997060000, 'cmc': 87595060000, 'ecmc': 25619951576, 'ecmc_psh': 803133, 'ha_shm': 1181857851, 'fc_rights': 228.35, 'bc_rights': 228.35, 'lxr_fc_rights': 228.35, 'stockCode': '000596'}]}

        if 'error' in content and content.get('error').get('message') == "Illegal token.":
            # 日志记录失败
            msg = '使用无效TOKEN ' + token + ' ' + '执行 collect_a_special_date_estimation 获取股票代码 ' + \
                  str(list(stock_info_dicts.keys())) + ' ' + str(date) + ' 时失败'
            custom_logger.CustomLogger().log_writter(msg, 'error')
            return self.collect_a_special_date_estimation(stock_info_dicts, date, exchange_location_mic)

        # 获取股票代码和名称的对应字典表
        # 如 {'000001': '平安银行', '000002': '万科A', '000031': '大悦城'}
        stock_code_name_dict = self.get_stock_code_name(stock_info_dicts)

        # 理杏仁接口返回为空，即 该日期，1/多个股票 没有估值数据，可能是因为 非交易日等原因造成，不予以储存
        if(len(content.get("data"))==0):
            # 日志记录
            msg = str(stock_code_name_dict) + " " + str(date)+" 的估值数据为空，不予以存储"
            custom_logger.CustomLogger().log_writter(msg, 'info')
            return

        try:
            # 数据存入数据库
            self.save_content_into_db(content, stock_info_dicts, "date")
            # 日志记录
            msg = "已经收集了 " + exchange_location_mic + "." + str(stock_code_name_dict) + " " + str(date) + " 的估值数据"
            custom_logger.CustomLogger().log_writter(msg, 'info')
        except Exception as e:
            # 日志记录失败
            msg = '数据存入数据库失败。 ' + '理杏仁接口返回为 ' + str(content) + '。 抛错为 ' + str(e) + ' 使用的token为 ' + token
            custom_logger.CustomLogger().log_writter(msg, 'error')


    def save_content_into_db(self, content, stock_info_dicts, range):
        # 将 理杏仁接口返回的数据 存入数据库
        # param:  content, 理杏仁接口返回的数据
        # param:  stock_codes_names_dict 股票代码名称字典, 可以1支/多支股票，
        # 如  {{'000001': {'stock_code': '000001', 'stock_name': '平安银行', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
        #      '000002': {'stock_code': '000002', 'stock_name': '万科A', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}}
        # param： range， 时间范围，只能填 period 或者 date

        # 解析返回的数据
        for piece in content["data"]:
            stock_code = piece['stockCode']
            stock_name = stock_info_dicts.get(stock_code).get("stock_name")
            date = piece['date'][:10]
            # 上市地
            exchange_location = stock_info_dicts.get(stock_code).get("exchange_location")
            # 交易所mic码
            exchange_location_mic = stock_info_dicts.get(stock_code).get("exchange_location_mic")

            # 检查记录是否已在数据库中存在
            is_data_existing = self.is_existing(stock_code, stock_name, date)
            # 如果已存在，且获取的是，一只股在一段时间内的估值数据，则循环截止,不需要收集这一天和往前日期的数据
            if is_data_existing and range=="period":
                # 日志记录
                msg = stock_code + " "+ stock_name+ "在"+ date+" 之前的估值数据已经存在，无需再存此日期之前的数据。"
                custom_logger.CustomLogger().log_writter(msg, 'info')
                break
            # 如果已存在，且获取的是，一/多只股在特定日期的估值数据，则停止收集该只数据，切换至下一个
            elif is_data_existing and range=="date":
                # 日志记录
                msg = stock_code + " " + stock_name + date + "的估值数据已经存在，无需再存此日期的数据。"
                custom_logger.CustomLogger().log_writter(msg, 'info')
                continue

            # 如果获取不到值，则置为0，避免出现 keyerror
            pe_ttm = piece.setdefault('pe_ttm', 0)
            pe_ttm_nonrecurring = piece.setdefault('d_pe_ttm', 0)
            pb = piece.setdefault('pb', 0)
            pb_wo_gw = piece.setdefault('pb_wo_gw', 0)
            ps_ttm = piece.setdefault('ps_ttm', 0)
            pcf_ttm = piece.setdefault('pcf_ttm', 0)
            ev_ebit = piece.setdefault('ev_ebit_r', 0)
            stock_yield = piece.setdefault('ey', 0)
            dividend_yield = piece.setdefault('dyr', 0)
            share_price = piece.setdefault('sp', 0)
            turnover = piece.setdefault('tv', 0)
            fc_rights = piece.setdefault('fc_rights', 0)
            bc_rights = piece.setdefault('bc_rights', 0)
            lxr_fc_rights = piece.setdefault('lxr_fc_rights', 0)
            shareholders = piece.setdefault('shn', 0)
            market_capitalization = piece.setdefault('mc', 0)
            circulation_market_capitalization = piece.setdefault('cmc', 0)
            free_circulation_market_capitalization = piece.setdefault('ecmc', 0)
            free_circulation_market_capitalization_per_capita = piece.setdefault('ecmc_psh', 0)
            financing_balance = piece.setdefault('fb', 0)
            securities_balances = piece.setdefault('sb', 0)
            stock_connect_holding_amount = piece.setdefault('ha_shm', 0)

            # 存入数据库
            inserting_sql = "INSERT IGNORE INTO stocks_main_estimation_indexes_historical_data (stock_code, stock_name, date, exchange_location, exchange_location_mic, pe_ttm,pe_ttm_nonrecurring,pb,pb_wo_gw,ps_ttm,pcf_ttm,ev_ebit,stock_yield,dividend_yield,share_price,turnover,fc_rights,bc_rights,lxr_fc_rights,shareholders,market_capitalization,circulation_market_capitalization,free_circulation_market_capitalization,free_circulation_market_capitalization_per_capita,financing_balance,securities_balances,stock_connect_holding_amount,source ) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s' )" % (
            stock_code, stock_name, date, exchange_location, exchange_location_mic, pe_ttm, pe_ttm_nonrecurring, pb, pb_wo_gw, ps_ttm, pcf_ttm, ev_ebit,
            stock_yield, dividend_yield, share_price, turnover, fc_rights, bc_rights, lxr_fc_rights, shareholders,
            market_capitalization, circulation_market_capitalization, free_circulation_market_capitalization,
            free_circulation_market_capitalization_per_capita, financing_balance, securities_balances,
            stock_connect_holding_amount,"理杏仁API")
            db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)

        # 日志记录
        #msg = str(stock_codes_names_dict) + '\'s estimation info has been saved '
        #custom_logger.CustomLogger().log_writter(msg, 'info')

    def is_existing(self, stock_code, stock_name, date):
        # 检查数据库中是否有记录，主要是检查是否为同一支股票同一日期
        # param: stock_code, 股票代码
        # param: stock_name，股票名称
        # param: date，日期
        # 输出：True，已存在； False，无记录

        selecting_sql = """SELECT pe_ttm FROM stocks_main_estimation_indexes_historical_data 
        where stock_code = '%s' and stock_name = '%s' and date = '%s' """ % (stock_code, stock_name, date)
        existing = db_operator.DBOperator().select_one("financial_data", selecting_sql)

        # 如果查询结果为None，则说明不存在；否则，说明有记录
        if existing == None:
            return False
        else:
            return True

    def latest_collection_date(self,exchange_location_mic):
        '''
        # 获取数据库中最新收集股票估值信息的日期
        # 返回 如果数据库有最新的日期，则返回最新收集股票估值信息的日期，类型为datetime.date， 如 2021-05-14
        #     如果数据库无最新的日期，返回起始日期 2010-01-01
        :param exchange_location_mic: 交易所MIC码（如XSHG, XSHE，XHKG）均可， 大小写均可
        :return:
        '''

        selecting_sql = """SELECT max(date) as date FROM stocks_main_estimation_indexes_historical_data 
                            where exchange_location_mic = '%s' """ % (exchange_location_mic)
        latest_collection_date = db_operator.DBOperator().select_one("financial_data", selecting_sql)
        # 如果数据库无最新的日期，为空
        if latest_collection_date['date'] == None:
            # 返回起始日期，2010-01-01
            return self.estimation_start_date
        return latest_collection_date['date']


    def collect_the_lacking_dates_estimation(self, saved_stock_info_dict, latest_collection_date, exchange_location_mic):
        '''
        收集 特定日期，数据库中暂时缺失的估值
        :param saved_stock_info_dict: 如
        # {{'000001': {'stock_code': '000001', 'stock_name': '平安银行', 'exchange_location': 'sz','exchange_location_mic': 'XSHE'},
        # '000002': {'stock_code': '000002', 'stock_name': '万科A', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},,,,}
        :param latest_collection_date: 数据库中最新收集股票估值信息的日期, 如 2022-04-11
        :param exchange_location_mic: 交易所MIC码（如XSHG, XSHE，XHKG）均可， 大小写均可
        :return:
        '''

        # 上市地代码或mic码，转为大写
        exchange_location_mic = exchange_location_mic.upper()

        plus_one_day = datetime.timedelta(days=1)
        # 数据库中的最大日期加一天
        start_date = latest_collection_date + plus_one_day
        today_date = date.today()
        # 如果日期在今天及之前
        while start_date <= today_date:
            # 调取理杏仁接口，获取特定一天，一只/多支股票估值数据, 并储存
            self.collect_a_special_date_estimation(saved_stock_info_dict, start_date, exchange_location_mic)
            # 日期递增一天
            start_date += plus_one_day


    def main(self):
        # 与上次数据库中待收集的股票代码和名称对比，
        # 并决定是 同时收集多只股票特定日期的数据 还是 分多次收集个股票一段时间的数据
        # param: start_date, 起始日期，如 2010-01-01

        # 获取所有跟踪股票的交易所代码，按交易所逐个收集股票估值信息
        # 如 ['XSHE', 'XHKG', 'XSHG']
        all_exchange_locaiton_mics = self.get_all_exchange_locaiton_mics()

        # 遍历交易所代码
        for exchange_location_mic in all_exchange_locaiton_mics:
            # 获取数据库中,该交易所最新收集股票估值信息的日期
            latest_collection_date = self.latest_collection_date(exchange_location_mic)
            # 如果最新收集日期与起始日期（2010-01-01）， 说明数据库为空，所有都需要从头开始收集全部股票的数据
            if(latest_collection_date==self.estimation_start_date):
                # 数据库中，全部需要被收集估值信息的股票
                # 需要被收集估值信息的股票代码，股票名称，上市地代码的字典
                # 如 [{'stock_code': '002688', 'stock_name': '金河生物', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
                #     {'stock_code': '603696', 'stock_name': '安记食品', 'exchange_location': 'sh', 'exchange_location_mic': 'XSHG'},
                #     {'stock_code': '002234', 'stock_name': '民和股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
                #     {'stock_code': '00700', 'stock_name': '腾讯控股', 'exchange_location': 'hk', 'exchange_location_mic': 'XHKG'},,, , , ]
                exchange_location_stock_info_list = self.all_tracking_stocks(exchange_location_mic)
                for stock_info in exchange_location_stock_info_list:
                    # stock_info_dict 股票代码, 名称, 上市地 字典, 只能1支股票，
                    #  如  {'000001': {'stock_code': '000001', 'stock_name': '平安银行', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}}
                    stock_code = stock_info.get("stock_code")
                    stock_info_dict = {stock_code: stock_info}
                    # 调取理杏仁接口，获取单个股票一段时间范围内，该股票估值数据, 并储存
                    self.collect_a_period_time_estimation(stock_code, stock_info_dict, self.estimation_start_date, self.today,
                                                          exchange_location_mic)
                # 日志记录
                msg = "数据库中的估值数据为空，已经收集了所有标的股票从 " +self.estimation_start_date+" 至 "+ str(self.today) +" 的估值数据"
                custom_logger.CustomLogger().log_writter(msg, 'info')

            else:
                # 某个交易所，需要被跟踪, 但暂时不在数据库中的股票个数
                not_saved_stocks_info_counter = self.the_stocks_that_not_in_db_counter(exchange_location_mic,latest_collection_date)
                # 如果存在 未被跟踪的股票, 则挨个，从 开始日期（2010-01-01）收集至今
                if not_saved_stocks_info_counter>0:
                    # not_saved_stocks_info 如
                    # [{'stock_code': '000858', 'stock_name': '五粮液', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
                    # {'stock_code': '300146', 'stock_name': '汤臣倍健', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},，，，]
                    not_saved_stock_info_list = self.the_stocks_that_not_in_db(exchange_location_mic, latest_collection_date)
                    # stock_info, 如 {'stock_code': '000001', 'stock_name': '平安银行', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}
                    for stock_info in not_saved_stock_info_list:
                        # stock_info_dict 股票代码, 名称, 上市地 字典, 只能1支股票，
                        #  如  {'000001': {'stock_code': '000001', 'stock_name': '平安银行', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}}
                        stock_code = stock_info.get("stock_code")
                        stock_info_dict = {stock_code : stock_info}

                        # 日志记录
                        msg = "有新增的标的股票" + str(stock_info) + "  需要收集从 " + self.estimation_start_date + " 至 " + str(self.today) + " 的估值数据"
                        custom_logger.CustomLogger().log_writter(msg, 'info')

                        # 调取理杏仁接口，获取单个股票一段时间范围内，该股票估值数据, 并储存
                        self.collect_a_period_time_estimation(stock_code, stock_info_dict, self.estimation_start_date, self.today, exchange_location_mic)


                # 某个交易所，获取数据库中已有的, 且也是那些需要被跟踪的股票的个数
                saved_stocks_info_counter = self.the_stocks_that_already_in_db_counter(exchange_location_mic,latest_collection_date)
                # saved_stock_info_list 如，
                # [{'stock_code': '000858', 'stock_name': '五粮液', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},
                # {'stock_code': '300146', 'stock_name': '汤臣倍健', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},，，，]
                saved_stock_info_list = self.the_stocks_that_already_in_db(exchange_location_mic, latest_collection_date)

                if saved_stocks_info_counter>0:
                    # 如果少于 每次向杏理仁请求数据时
                    if saved_stocks_info_counter < self.page_size:
                        saved_stock_info_dict = dict()
                        for stock_info in saved_stock_info_list:
                            stock_code = stock_info.get("stock_code")
                            saved_stock_info_dict[stock_code] = stock_info
                        # 此时 saved_stock_info_dict 如
                        # {{'000001': {'stock_code': '000001', 'stock_name': '平安银行', 'exchange_location': 'sz','exchange_location_mic': 'XSHE'},
                        # '000002': {'stock_code': '000002', 'stock_name': '万科A', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},,,,}
                        self.collect_the_lacking_dates_estimation(saved_stock_info_dict, latest_collection_date,
                                                             exchange_location_mic)

                    else:
                        # 根据 每次申请的条数，需要分成多少次向杏理仁请求数据，一次性超过100条的话，会被理杏仁拒绝

                        # 如果总数除以每次申请条数有余数，则总页数需要多加一页
                        if (saved_stocks_info_counter % self.page_size != 0):
                            # 需要分成多少页数
                            total_page_num = saved_stocks_info_counter // self.page_size + 1
                        # 如果总数除以每次申请条数无余数，刚刚好
                        else:
                            # 需要分成多少页数
                            total_page_num = saved_stocks_info_counter // self.page_size
                        # 起始list的标码
                        start_index = 0
                        # 结束list的标码
                        end_index = self.page_size
                        # 遍历这些页码
                        for page_num in range(total_page_num):
                            saved_stock_info_list_piece = saved_stock_info_list[start_index:end_index]
                            saved_stock_info_dict_piece = dict()
                            for stock_info in saved_stock_info_list_piece:
                                stock_code = stock_info.get("stock_code")
                                saved_stock_info_dict_piece[stock_code] = stock_info
                            # 此时 saved_stock_info_dict_piece 如
                            # {{'000001': {'stock_code': '000001', 'stock_name': '平安银行', 'exchange_location': 'sz','exchange_location_mic': 'XSHE'},
                            # '000002': {'stock_code': '000002', 'stock_name': '万科A', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'},,,,}
                            start_index = end_index
                            end_index = end_index + self.page_size
                            self.collect_the_lacking_dates_estimation(saved_stock_info_dict_piece, latest_collection_date,
                                                                      exchange_location_mic)



if __name__ == "__main__":
    time_start = time.time()
    go = CollectStockHistoricalEstimationInfo()
    #result = go.get_all_exchange_locaiton_mics()
    #print(result)
    #result = go.all_tracking_stocks("XHKG")
    #print(stock_codes_names_dict)
    #go.collect_a_period_time_estimation("600900", {"600900":{"stock_code": "600900", "stock_name": "长江电力", "exchange_location": "sh", "exchange_location_mic": "XSHG"}}, "2010-01-01", "2023-05-07", "XSHG")
    #go.collect_a_period_time_estimation('000002', {'000002': {'stock_code': '000002', 'stock_name': '万科A', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}}, "2022-04-01", "2022-04-11", 'XSHE')
    #saved_stock_info_dict = {'000858': {'stock_code': '000858', 'stock_name': '五粮液', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002714': {'stock_code': '002714', 'stock_name': '牧原股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000568': {'stock_code': '000568', 'stock_name': '泸州老窖', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '300498': {'stock_code': '300498', 'stock_name': '温氏股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002304': {'stock_code': '002304', 'stock_name': '洋河股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000895': {'stock_code': '000895', 'stock_name': '双汇发展', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000876': {'stock_code': '000876', 'stock_name': '新希望', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002385': {'stock_code': '002385', 'stock_name': '大北农', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000596': {'stock_code': '000596', 'stock_name': '古井贡酒', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '300999': {'stock_code': '300999', 'stock_name': '金龙鱼', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '300146': {'stock_code': '300146', 'stock_name': '汤臣倍健', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000998': {'stock_code': '000998', 'stock_name': '隆平高科', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002507': {'stock_code': '002507', 'stock_name': '涪陵榨菜', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002557': {'stock_code': '002557', 'stock_name': '洽洽食品', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002568': {'stock_code': '002568', 'stock_name': '百润股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002299': {'stock_code': '002299', 'stock_name': '圣农发展', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000930': {'stock_code': '000930', 'stock_name': '中粮科技', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002511': {'stock_code': '002511', 'stock_name': '中顺洁柔', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002157': {'stock_code': '002157', 'stock_name': '正邦科技', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000729': {'stock_code': '000729', 'stock_name': '燕京啤酒', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002124': {'stock_code': '002124', 'stock_name': '天邦股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002458': {'stock_code': '002458', 'stock_name': '益生股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000869': {'stock_code': '000869', 'stock_name': '张裕A', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '300741': {'stock_code': '300741', 'stock_name': '华宝股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002946': {'stock_code': '002946', 'stock_name': '新乳业', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002311': {'stock_code': '002311', 'stock_name': '海大集团', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000799': {'stock_code': '000799', 'stock_name': '酒鬼酒', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000860': {'stock_code': '000860', 'stock_name': '顺鑫农业', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002597': {'stock_code': '002597', 'stock_name': '金禾实业', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '300957': {'stock_code': '300957', 'stock_name': '贝泰妮', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002505': {'stock_code': '002505', 'stock_name': '鹏都农牧', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002626': {'stock_code': '002626', 'stock_name': '金达威', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002481': {'stock_code': '002481', 'stock_name': '双塔食品', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002216': {'stock_code': '002216', 'stock_name': '三全食品', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '300761': {'stock_code': '300761', 'stock_name': '立华股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '300973': {'stock_code': '300973', 'stock_name': '立高食品', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '300783': {'stock_code': '300783', 'stock_name': '三只松鼠', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002461': {'stock_code': '002461', 'stock_name': '珠江啤酒', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000002': {'stock_code': '000002', 'stock_name': '万科A', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '001979': {'stock_code': '001979', 'stock_name': '招商蛇口', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000069': {'stock_code': '000069', 'stock_name': '华侨城A', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000656': {'stock_code': '000656', 'stock_name': '金科股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000540': {'stock_code': '000540', 'stock_name': '中天金融', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002146': {'stock_code': '002146', 'stock_name': '荣盛发展', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000961': {'stock_code': '000961', 'stock_name': '中南建设', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002244': {'stock_code': '002244', 'stock_name': '滨江集团', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000402': {'stock_code': '000402', 'stock_name': '金融街', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '001914': {'stock_code': '001914', 'stock_name': '招商积余', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000718': {'stock_code': '000718', 'stock_name': '苏宁环球', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000671': {'stock_code': '000671', 'stock_name': '阳光城', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000031': {'stock_code': '000031', 'stock_name': '大悦城', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000001': {'stock_code': '000001', 'stock_name': '平安银行', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002142': {'stock_code': '002142', 'stock_name': '宁波银行', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002936': {'stock_code': '002936', 'stock_name': '郑州银行', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002966': {'stock_code': '002966', 'stock_name': '苏州银行', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002958': {'stock_code': '002958', 'stock_name': '青农商行', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002839': {'stock_code': '002839', 'stock_name': '张家港行', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002807': {'stock_code': '002807', 'stock_name': '江阴银行', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002948': {'stock_code': '002948', 'stock_name': '青岛银行', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002646': {'stock_code': '002646', 'stock_name': '天佑德酒', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002582': {'stock_code': '002582', 'stock_name': '好想你', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002746': {'stock_code': '002746', 'stock_name': '仙坛股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002100': {'stock_code': '002100', 'stock_name': '天康生物', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002567': {'stock_code': '002567', 'stock_name': '唐人神', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '000848': {'stock_code': '000848', 'stock_name': '承德露露', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002688': {'stock_code': '002688', 'stock_name': '金河生物', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}, '002234': {'stock_code': '002234', 'stock_name': '民和股份', 'exchange_location': 'sz', 'exchange_location_mic': 'XSHE'}}
    #go.collect_a_special_date_estimation(saved_stock_info_dict, "2022-04-13", "XSHE")
    # print(content)
    #result = go.is_existing("000568", "泸州老窖", "2020-11-19")
    #print(result)
    go.main()
    #print(go.all_tracking_stocks_counter("XHKG"))
    #go.latest_collection_date("2021-04-01")
    #go.test_date_loop()
    #result = go.paged_demanded_stocks("XSHE",0,80)
    #result = go.tell_exchange_market_and_determine_url("XHKG")
    #result = go.page_counter_by_page_size_per_page("XSHG")
    #result = go.tell_exchange_market_and_determine_what_estimations_need_to_get("XHKG")
    #result = go.the_stocks_that_already_in_db("XSHE", "2022-04-12")
    #result = go.the_stocks_that_already_in_db_counter("XSHE", "2022-04-12")
    #result = go.the_stocks_that_not_in_db("XSHE", "2022-04-12")
    #print(result)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))
