#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

import time

import sys

sys.path.append("..")
import log.custom_logger as custom_logger
import database.db_operator as db_operator
import db_mapper.target_pool.investment_target_mapper as investment_target_mapper


class WebServericeImpl:
    '''
    通过接口，网络与该决策系统互动的实现
    执行层
    '''

    def __init__(self):
        pass

    def __check_exchange_location_param(self, exchange_location):
        '''
        检查上市地参数
        :param exchange_location: 标的上市地, 如 sz, sh, hk， 必选
        :return: 正确的话，返回交易所的MIC码；错误的话，返回None
        '''
        # 深圳证券交易所
        if (exchange_location == "sz"):
            exchange_location_mic = "XSHE"
        # 上海证券交易所
        elif (exchange_location == "sh"):
            exchange_location_mic = "XSHG"
        # 香港证券交易所
        elif (exchange_location == "hk"):
            exchange_location_mic = "XHKG"
        else:
            return None
        return exchange_location_mic

        """
            # 暂不支持美股监控
            # 纽约证券交易所
            elif(operation_target_params.exchange_location == "ny"):
                exchange_location_mic = "XNYS"
            # 纳斯达克
            elif (operation_target_params.exchange_location == "ny"):
                exchange_location_mic = "XNAS"
            # 美国证券交易所
            elif (operation_target_params.exchange_location == "ny"):
                exchange_location_mic = "XASE"
        """

    def __check_valuation_method(self, valuation_method):
        '''
        检查估值策略是否正确,
        :param valuation_method:
        :return: boolean，True或者False
        '''
        # 是否为 市净率，市盈率，市销率，股息率，净资产回报率，peg策略
        if (valuation_method in ["pb", "pe_ttm", "ps_ttm", "dr", "roe", "peg"]):
            return True
        else:
            return False

    def __check_monitoring_frequency(self, monitoring_frequency):
        '''
        检查监控频率是否正确
        :param monitoring_frequency:
        :return: boolean，True或者False
        '''
        # 是否为 秒级，分级，时级，日级，周级，月级，季级，年级，周期级
        if (monitoring_frequency in ["secondly", "minutely", "hourly", "daily", "weekly", "monthly", "seasonally",
                                     "yearly", "periodically"]):
            return True
        else:
            return False

    def __is_a_num(self, num_str):
        '''
        检查一个数是否为数字，包含整数，小数
        :param num_str:输入一个为str
        :return:
        '''
        # 清除前后空格
        num = num_str.strip()
        try:
            float(num)
            return True
        except ValueError:
            pass
        return False

    def __is_status_right(self, status):
        '''
        检查 标的策略状态 是否传入正确
        :param status: 标的策略状态，目前仅 active，suspend，inactive 三个状态
        :return:
        '''
        if status in ("active", "suspend", "inactive"):
            return True
        else:
            return False

    def __is_hold_or_not_right(self, hold_or_not):
        '''
        检查 当前是否持有 参数是否输入政策
        :param hold_or_not: 是否持有， 1为持有，0不持有
        :return:
        '''
        try:
            if (int(hold_or_not) == 0 or int(hold_or_not) == 1):
                return True
        except ValueError:
            pass
        return False

    def __is_trade_right(self, trade):
        '''
        检查交易方向是否正确
        :param trade: 买入-buy, 卖出-sell
        :return:
        '''
        if (trade == 'buy' or trade == 'sell'):
            return True
        else:
            return False

    def operate_target_impl(self, operation_target_params):
        '''
        实现对标的物进行操作，支持更新，创建
        创建 create之后，operation: 操作， target_type: 标的类型， target_code: 标的代码， exchange_location: 标的上市地，
        valuation_method: 估值策略， monitoring_frequency: 监控频率， holder: 标的持有人 这些参数不可被更新update；

        可被更新update的参数，有 target_name: 跟踪标的名称， trigger_value: 估值触发绝对值值临界点， trigger_percent: 估值触发历史百分比临界点，
        status: 标的策略状态， hold_or_not：当前是否持有， index_company: 指数开发公司， buy_and_hold_strategy: 买入持有策略，
        sell_out_strategy: 卖出策略

        :param operation_target_params.operation: 操作，如 create, update, 必选
        :param operation_target_params.target_type: 标的类型，如 index, stock，必选
        :param operation_target_params.target_code: 标的代码，如 指数代码 399997，股票代码 600519，必选
        :param operation_target_params.exchange_location: 标的上市地, 如 sz, sh, hk， 必选
        :param operation_target_params.trade: 交易方向, 买入-buy, 卖出-sell， 必选
        :param operation_target_params.valuation_method: 估值策略, 如 pb,pe,ps， 必选
        :param operation_target_params.monitoring_frequency: 监控频率, secondly, minutely, hourly, daily, weekly, monthly, seasonally, yearly, periodically， 必选
        :param operation_target_params.holder: 标的持有人， 必选

        :param operation_target_params.target_name: 跟踪标的名称，如 中证白酒指数, 万科， 创建时-必选，更新时-可选
        :param operation_target_params.trigger_value: 估值触发绝对值值临界点，含等于，看指标具体该大于等于还是小于等于，如 pb估值时，0.95, 创建时-必选，更新时-可选
        :param operation_target_params.trigger_percent: 估值触发历史百分比临界点，含等于，看指标具体该大于等于还是小于等于，如 10，即10%位置 创建时-必选，更新时-可选
        :param operation_target_params.status: 标的策略状态，如 active，suspend，inactive  创建时-必选，更新时-可选
        :param operation_target_params.hold_or_not：当前是否持有,1为持有，0不持有 创建时-必选，更新时-可选
        :param operation_target_params.index_company: 指数开发公司, 如中证，国证， 创建指数标的时-必选，更新指数标的时-可选

        :param operation_target_params.buy_and_hold_strategy: 买入持有策略, 创建时-可选，更新时-可选
        :param operation_target_params.sell_out_strategy: 卖出策略, 创建时-可选，更新时-可选

        :return:
        '''

        # 获取当前日期
        today = time.strftime("%Y-%m-%d", time.localtime())

        # 检查必选参数是否有传入

        # 操作是否传入正确
        if (
                operation_target_params.operation != "create" and operation_target_params.operation != "update" or operation_target_params.operation == None):
            return {"msg": "操作参数operation出错", "code": 400, "status": "Failure"}

        # 标的类型是否传入正确
        if (
                operation_target_params.target_type != "index" and operation_target_params.target_type != "stock" or operation_target_params.target_type == None):
            return {"msg": "标的类型参数target_type出错", "code": 400, "status": "Failure"}

        # 标的代码是否传入
        if (operation_target_params.target_code == None):
            return {"msg": "标的代码参数target_code出错", "code": 400, "status": "Failure"}

        # 检查上市地参数是否合乎规范
        exchange_location_mic = self.__check_exchange_location_param(operation_target_params.exchange_location)
        if (exchange_location_mic == None):
            return {"msg": "上市地参数exchange_location出错", "code": 400, "status": "Failure"}

        # 检查估值策略是否正确
        is_valuation_method_right = self.__check_valuation_method(operation_target_params.valuation_method)
        if (not is_valuation_method_right):
            return {"msg": "估值策略参数valuation_method出错", "code": 400, "status": "Failure"}

        # 检查监控频率是否正确
        is_monitoring_frequency_right = self.__check_monitoring_frequency(
            operation_target_params.monitoring_frequency)
        if (not is_monitoring_frequency_right):
            return {"msg": "监控频率参数monitoring_frequency出错", "code": 400, "status": "Failure"}

        # 标的持有人是否传入
        if (operation_target_params.holder == None):
            return {"msg": "标的持有人参数holder出错", "code": 400, "status": "Failure"}

        # 交易方向
        if (not self.__is_trade_right(operation_target_params.trade)):
            return {"msg": "交易方向参数trade出错", "code": 400, "status": "Failure"}

        #####################################    create   #####################################################

        # 如果是创建新标的
        if (operation_target_params.operation == "create"):

            # 跟踪标的名称是否为空
            if (operation_target_params.target_name == None):
                return {"msg": "标的名称参数target_name为空", "code": 400, "status": "Failure"}

            # 估值触发绝对值值临界点是否为空
            if (operation_target_params.trigger_value == None):
                return {"msg": "估值触发绝对值值临界点参数trigger_value为空", "code": 400, "status": "Failure"}
            # 估值触发绝对值值临界点是否为数字
            if (not self.__is_a_num(operation_target_params.trigger_value)):
                return {"msg": "估值触发绝对值值临界点参数trigger_value不是数值", "code": 400, "status": "Failure"}

            # 估值触发历史百分比临界点是否为空
            if (operation_target_params.trigger_percent == None):
                return {"msg": "指数开发公司参数trigger_percent为空", "code": 400, "status": "Failure"}
            # 估值触发历史百分比临界点是否为数字
            if (not self.__is_a_num(operation_target_params.trigger_percent)):
                return {"msg": "指数开发公司参数trigger_percent不是数值", "code": 400, "status": "Failure"}

            # 检查 标的策略状态 是否传入正确
            if (not self.__is_status_right(operation_target_params.status)):
                return {"msg": "标的策略状态参数status出错，请从 active，suspend，inactive 中选择", "code": 400, "status": "Failure"}

            # 检查 当前是否持有 是否传入正确
            if (not self.__is_hold_or_not_right(operation_target_params.hold_or_not)):
                return {"msg": "当前是否持有参数hold_or_not出错，请从 0，1（1-持有，0-不持有） 中选择", "code": 400, "status": "Failure"}

            #####################################    create`index   #################################
            if (operation_target_params.target_type == "index"):

                # 指数开发公司是否为空
                if (operation_target_params.index_company == None):
                    return {"msg": "指数开发公司参数index_company为空", "code": 400, "status": "Failure"}
                # 存储标的指数的信息
                is_inserted_successfully_dict = investment_target_mapper.InvestmentTargetMapper().save_target_index_info(
                    operation_target_params.target_type, operation_target_params.target_code,
                    operation_target_params.target_name, operation_target_params.index_company,
                    operation_target_params.exchange_location, exchange_location_mic,
                    operation_target_params.hold_or_not,
                    operation_target_params.valuation_method, operation_target_params.trigger_value,
                    operation_target_params.trigger_percent, operation_target_params.buy_and_hold_strategy,
                    operation_target_params.sell_out_strategy, operation_target_params.trade,
                    operation_target_params.monitoring_frequency,
                    operation_target_params.holder, operation_target_params.status, today)

                # 如果插入成功
                if (is_inserted_successfully_dict.get("status")):
                    # 日志记录
                    msg = '创建新的指数标的-' + operation_target_params.target_name + '-成功'
                    return {"msg": msg, "code": 200, "status": "Success"}
                # 如果插入失败
                else:
                    # 日志记录
                    msg = '创建新的指数标的-' + operation_target_params.target_name + '-失败 ' + is_inserted_successfully_dict.get(
                        "msg")
                    custom_logger.CustomLogger().log_writter(msg, 'error')
                    return {"msg": msg, "code": 400, "status": "Failure"}

            #####################################    create`stock   ###############################################

            elif (operation_target_params.target_type == "stock"):
                # 存储标的股票的信息
                is_inserted_successfully_dict = investment_target_mapper.InvestmentTargetMapper().save_target_stock_info(operation_target_params.target_type, operation_target_params.target_code,
                                   operation_target_params.target_name,
                                   operation_target_params.exchange_location, exchange_location_mic,
                                   operation_target_params.hold_or_not,
                                   operation_target_params.valuation_method, operation_target_params.trigger_value,
                                   operation_target_params.trigger_percent,
                                   operation_target_params.buy_and_hold_strategy,
                                   operation_target_params.sell_out_strategy, operation_target_params.trade,
                                   operation_target_params.monitoring_frequency,
                                   operation_target_params.holder, operation_target_params.status, today)

                # 如果插入成功
                if (is_inserted_successfully_dict.get("status")):
                    # 日志记录
                    msg = '创建新的股票标的-' + operation_target_params.target_name + '-成功'
                    return {"msg": msg, "code": 200, "status": "Success"}
                # 如果插入失败
                else:
                    # 日志记录
                    msg = '创建新的股票标的' + operation_target_params.target_name + '失败 ' + is_inserted_successfully_dict.get(
                        "msg")
                    custom_logger.CustomLogger().log_writter(msg, 'error')
                    return {"msg": msg, "code": 400, "status": "Failure"}


        #####################################    update   #####################################################

        # 如果是更新标的
        elif (operation_target_params.operation == "update"):

            # 拼接的动态sql
            dynamic_sql = "SET p_day = %(today)s"
            # 待更新的参数值
            params_dict = {"today": today}

            # 如果标的名称不为空，则需要更新
            if (operation_target_params.target_name != None):
                dynamic_sql += ", target_name=%(target_name)s"
                params_dict["target_name"] = operation_target_params.target_name

            # 如果估值触发绝对值值临界点不为空
            if (operation_target_params.trigger_value != None):
                # 估值触发绝对值值临界点是否为数字
                # 不是数字
                if (not self.__is_a_num(operation_target_params.trigger_value)):
                    return {"msg": "估值触发绝对值值临界点参数trigger_value不是数值", "code": 400, "status": "Failure"}
                # 是数字，则需要更新
                else:
                    dynamic_sql += ", trigger_value=%(trigger_value)s"
                    params_dict["trigger_value"] = operation_target_params.trigger_value

            # 如果估值触发历史百分比临界点不为空
            if (operation_target_params.trigger_percent != None):
                # 估值触发历史百分比临界点是否为数字
                # 不是数字
                if (not self.__is_a_num(operation_target_params.trigger_percent)):
                    return {"msg": "估值触发绝对值值临界点参数trigger_percent不是数值", "code": 400, "status": "Failure"}
                # 是数字，则需要更新
                else:
                    dynamic_sql += ", trigger_percent=%(trigger_percent)s"
                    params_dict["trigger_percent"] = operation_target_params.trigger_percent

            # 如果买入持有策略不为空，则需要更新
            if (operation_target_params.buy_and_hold_strategy != None):
                dynamic_sql += ", buy_and_hold_strategy=%(buy_and_hold_strategy)s"
                params_dict["buy_and_hold_strategy"] = operation_target_params.buy_and_hold_strategy

            # 如果卖出策略不为空，则需要更新
            if (operation_target_params.sell_out_strategy != None):
                dynamic_sql += ", sell_out_strategy=%(sell_out_strategy)s"
                params_dict["sell_out_strategy"] = operation_target_params.sell_out_strategy

            # 如果标的策略状态不为空，则需要更新
            if (operation_target_params.status != None):
                dynamic_sql += ", status=%(status)s"
                params_dict["status"] = operation_target_params.status

            # 如果当前是否持有不为空，则需要更新
            if (operation_target_params.hold_or_not != None):
                dynamic_sql += ", hold_or_not=%(hold_or_not)s"
                params_dict["hold_or_not"] = operation_target_params.hold_or_not

            # 加入 必传参数，跟踪标的类型， 跟踪标的代码， 标的上市地，估值方法, 监控频率, 标的持有人, 交易方向
            params_dict["target_type"] = operation_target_params.target_type
            params_dict["target_code"] = operation_target_params.target_code
            params_dict["exchange_location"] = operation_target_params.exchange_location
            params_dict["valuation_method"] = operation_target_params.valuation_method
            params_dict["monitoring_frequency"] = operation_target_params.monitoring_frequency
            params_dict["holder"] = operation_target_params.holder
            params_dict["trade"] = operation_target_params.trade

            #####################################    update`index   ###################################################
            # 如果是更新 指数标的
            if (operation_target_params.target_type == "index"):
                # 如果指数开发公司不为空，则需要更新
                if (operation_target_params.index_company != None):
                    dynamic_sql += ", index_company=%(index_company)s"
                    params_dict["index_company"] = operation_target_params.index_company

                updating_sql = " UPDATE investment_target " + dynamic_sql + " WHERE target_type=%(target_type)s AND " \
                                                                            "target_code=%(target_code)s AND exchange_location=%(exchange_location)s AND " \
                                                                            "valuation_method=%(valuation_method)s AND monitoring_frequency=%(monitoring_frequency)s " \
                                                                            "AND holder=%(holder)s AND trade=%(trade)s"
                # 是否执行成功
                is_updated_successfully_dict = db_operator.DBOperator().operate("insert", "target_pool", updating_sql,
                                                                                params_dict)
                # 如果插入成功
                if (is_updated_successfully_dict.get("status")):
                    # 日志记录
                    msg = '更新指数标的-' + operation_target_params.target_code + '-成功'
                    return {"msg": msg, "code": 200, "status": "Success"}
                # 如果插入失败
                else:
                    # 日志记录
                    msg = '更新指数标的-' + operation_target_params.target_code + ' 失败 ' + is_updated_successfully_dict.get(
                        "msg")
                    custom_logger.CustomLogger().log_writter(msg, 'error')
                    return {"msg": msg, "code": 400, "status": "Failure"}

            #####################################    update`stock   ###################################################
            # 如果是更新 股票标的
            elif (operation_target_params.target_type == "stock"):
                updating_sql = " UPDATE investment_target " + dynamic_sql + " WHERE target_type=%(target_type)s AND " \
                                                                            "target_code=%(target_code)s AND exchange_location=%(exchange_location)s AND " \
                                                                            "valuation_method=%(valuation_method)s AND monitoring_frequency=%(monitoring_frequency)s " \
                                                                            "AND holder=%(holder)s AND trade=%(trade)s"
                # 是否执行成功
                is_updated_successfully_dict = db_operator.DBOperator().operate("insert", "target_pool", updating_sql,
                                                                                params_dict)
                # 如果插入成功
                if (is_updated_successfully_dict.get("status")):
                    # 日志记录
                    msg = '更新股票标的-' + operation_target_params.target_code + '-成功'
                    return {"msg": msg, "code": 200, "status": "Success"}
                # 如果插入失败
                else:
                    # 日志记录
                    msg = '更新股票标的-' + operation_target_params.target_code + ' 失败 ' + is_updated_successfully_dict.get(
                        "msg")
                    custom_logger.CustomLogger().log_writter(msg, 'error')
                    return {"msg": msg, "code": 400, "status": "Failure"}

    def mute_impl(self, operation_target_params):
        '''
        暂停标的策略
        :param operation_target_params:
                        target_type: 标的类型，如 index, stock，必选
                        target_code: 标的代码，如 指数代码 399997，股票代码 600519，必选
        :return:
        '''
        updating_sql = " UPDATE investment_target SET status = 'inactive' WHERE  target_type = '%s' AND target_code = '%s' " \
                       % (operation_target_params.target_type, operation_target_params.target_code)
        # 是否执行成功
        is_updated_successfully_dict = db_operator.DBOperator().operate("insert", "target_pool", updating_sql)
        # 如果插入成功
        if (is_updated_successfully_dict.get("status")):
            # 日志记录
            msg = '暂停跟踪投资标的-' + operation_target_params.target_code + '-成功'
            custom_logger.CustomLogger().log_writter(msg, 'info')
            return {"msg": msg, "code": 200, "status": "Success"}
        # 如果插入失败
        else:
            # 日志记录
            msg = '暂停跟踪投资标的-' + operation_target_params.target_code + ' 失败 ' + is_updated_successfully_dict.get(
                "msg")
            custom_logger.CustomLogger().log_writter(msg, 'error')
            return {"msg": msg, "code": 400, "status": "Failure"}

    def restart_all_mute_target(self):
        '''
        将所有暂停标的策略重新开启，下一个交易日又可生效
        :return:
        '''
        updating_sql = " UPDATE investment_target SET status = 'active' WHERE  status = 'inactive' "
        # 是否执行成功
        is_updated_successfully_dict = db_operator.DBOperator().operate("insert", "target_pool", updating_sql)
        # 如果插入成功
        if (is_updated_successfully_dict.get("status")):
            # 日志记录
            msg = '重新跟踪所有被暂停的投资标的-成功'
            custom_logger.CustomLogger().log_writter(msg, 'info')
            return {"msg": msg, "code": 200, "status": "Success"}
        # 如果插入失败
        else:
            # 日志记录
            msg = '重新跟踪所有被暂停的投资标的-失败 ' + is_updated_successfully_dict.get("msg")
            custom_logger.CustomLogger().log_writter(msg, 'error')
            return {"msg": msg, "code": 400, "status": "Failure"}


if __name__ == '__main__':
    go = WebServericeImpl()
    go.restart_all_mute_target()
