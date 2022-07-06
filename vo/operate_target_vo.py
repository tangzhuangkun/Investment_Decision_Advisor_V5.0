#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

class OperateTargetVo:
    '''
    对标的物进行操作，支持更新，创建
    所需用到的参数
    '''

    def __init__(self, target_type, operation, target_code, target_name, index_company, valuation_method, trigger_value,
                 trigger_percent, buy_and_hold_strategy, sell_out_strategy,
                 monitoring_frequency, holder, status,exchange_location, hold_or_not, trade):
        self._target_type = target_type
        self._operation = operation
        self._target_code = target_code
        self._target_name = target_name
        self._index_company = index_company
        self._valuation_method = valuation_method
        self._trigger_value = trigger_value
        self._trigger_percent = trigger_percent
        self._buy_and_hold_strategy = buy_and_hold_strategy
        self._sell_out_strategy = sell_out_strategy
        self._hold_or_not = hold_or_not
        self._monitoring_frequency = monitoring_frequency
        self._holder = holder
        self._status = status
        self._exchange_location = exchange_location
        self._trade = trade

    # 标的类型
    @property
    def target_type(self):
        return self._target_type
    @target_type.setter
    def target_type(self, target_type):
        self._target = target_type

    # 操作
    @property
    def operation(self):
        return self._operation
    @operation.setter
    def operation(self, operation):
        self._operation = operation

    # 标的代码
    @property
    def target_code(self):
        return self._target_code
    @target_code.setter
    def target_code(self, target_code):
        self._target_code = target_code

    # 标的名称
    @property
    def target_name(self):
        return self._target_name
    @target_name.setter
    def target_name(self, target_name):
        self._target_name = target_name

    # 指数开发公司
    @property
    def index_company(self):
        return self._index_company
    @index_company.setter
    def index_company(self, index_company):
        self._index_company = index_company

    # 估值策略
    @property
    def valuation_method(self):
        return self._valuation_method
    @valuation_method.setter
    def valuation_method(self, valuation_method):
        self._valuation_method = valuation_method

    # 估值触发绝对值值临界点
    @property
    def trigger_value(self):
        return self._trigger_value
    @trigger_value.setter
    def trigger_value(self, trigger_value):
        self._trigger_value = trigger_value

    # 估值触发历史百分比临界点
    @property
    def trigger_percent(self):
        return self._trigger_percent
    @trigger_percent.setter
    def trigger_percent(self, trigger_percent):
        self._trigger_percent = trigger_percent

    # 买入持有策略
    @property
    def buy_and_hold_strategy(self):
        return self._buy_and_hold_strategy
    @buy_and_hold_strategy.setter
    def buy_and_hold_strategy(self, buy_and_hold_strategy):
        self._buy_and_hold_strategy = buy_and_hold_strategy

    # 卖出策略
    @property
    def sell_out_strategy(self):
        return self._sell_out_strategy
    @sell_out_strategy.setter
    def sell_out_strategy(self, sell_out_strategy):
        self._sell_out_strategy = sell_out_strategy

    # 监控频率
    @property
    def monitoring_frequency(self):
        return self._monitoring_frequency
    @monitoring_frequency.setter
    def monitoring_frequency(self, monitoring_frequency):
        self._monitoring_frequency = monitoring_frequency

    # 标的持有人
    @property
    def holder(self):
        return self._holder
    @holder.setter
    def holder(self, holder):
        self._holder = holder

    # 标的策略状态
    @property
    def status(self):
        return self._status
    @status.setter
    def status(self, status):
        self._status = status

    # 标的上市地, 如 sz, sh, hk
    @property
    def exchange_location(self):
        return self._exchange_location
    @exchange_location.setter
    def exchange_location(self, exchange_location):
        self._exchange_location = exchange_location

    # 当前是否持有,1为持有，0不持有
    @property
    def hold_or_not(self):
        return self._hold_or_not
    @hold_or_not.setter
    def hold_or_not(self, hold_or_not):
        self._hold_or_not = hold_or_not

    # 交易方向，buy,sell
    @property
    def trade(self):
        return self._trade
    @trade.setter
    def trade(self, trade):
        self._trade = trade