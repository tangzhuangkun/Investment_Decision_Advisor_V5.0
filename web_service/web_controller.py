#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

# 通过接口，网络与该决策系统互动

import sys

sys.path.append('..')
import web_service.web_service_impl as web_service_impl
import vo.operate_target_vo as operate_target_vo
import vo.mute_target_vo as mute_target_vo
from flask import Flask, request, jsonify  # 引入request对象

app = Flask(__name__)
# 解决中文乱码问题
app.config['JSON_AS_ASCII'] = False
app.config['JSONIFY_MIMETYPE'] = "application/json;charset=utf-8"

@app.route("/operate_investment_target", methods=['GET','POST'])
def operate_target():
    '''
    对标的物进行操作，支持更新，创建
    :return:
    '''

    if request.method == 'POST':
        # 获取请求参数
        # 标的类型，如 index, stock
        target_type = request.args.get("target_type")
        # 操作，如 create, update
        operation = request.args.get("operation")
        # 标的代码，如 指数代码 399997，股票代码 600519
        target_code = request.args.get("target_code")
        # 跟踪标的名称，如 中证白酒指数, 万科
        target_name = request.args.get("target_name")
        # 指数开发公司，如 中证，国证
        index_company = request.args.get("index_company")
        # 标的上市地, 如 sz, sh, hk
        exchange_location = request.args.get("exchange_location")
        # 交易方向，如 bug，sell
        trade = request.args.get("trade")
        # 估值策略, 如 pb,pe,ps
        valuation_method = request.args.get("valuation_method")
        # 估值触发绝对值值临界点，含等于，看指标具体该大于等于还是小于等于，如 pb估值时，0.9
        trigger_value = request.args.get("trigger_value")
        # 估值触发历史百分比临界点，含等于，看指标具体该大于等于还是小于等于，如 10，即10%位置
        trigger_percent = request.args.get("trigger_percent")
        # 买入持有策略
        buy_and_hold_strategy = request.args.get("buy_and_hold_strategy")
        # 卖出策略
        sell_out_strategy = request.args.get("sell_out_strategy")
        # 当前是否持有,1为持有，0不持有
        hold_or_not = request.args.get("hold_or_not")
        # 监控频率, secondly, minutely, hourly, daily, weekly, monthly, seasonally, yearly, periodically
        monitoring_frequency = request.args.get("monitoring_frequency")
        # 标的持有人
        holder = request.args.get("holder")
        # 标的策略状态，如 active，suspend，inactive
        status = request.args.get("status")

        # 包装成一个对象
        params = operate_target_vo.OperateTargetVo(target_type, operation, target_code, target_name, index_company,
                                                   valuation_method, trigger_value, trigger_percent, buy_and_hold_strategy,
                                                   sell_out_strategy, monitoring_frequency, holder, status,
                                                   exchange_location, hold_or_not,trade)
        # 实现 更新，创建
        return jsonify(web_service_impl.WebServericeImpl().operate_target_impl(params))
    else:
        return jsonify({"msg": "调用方式出错", "code": 400})


@app.route("/mute", methods=['GET', 'POST'])
def mute():
    '''
    暂停标的策略
    :return:
    '''
    if request.method == 'POST':
        # 标的类型，如 index, stock
        target_type = request.args.get("target_type")
        # 标的代码，如 指数代码 399997，股票代码 600519
        target_code = request.args.get("target_code")
        # 跟踪标的名称，如 中证白酒指数, 万科
        target_name = request.args.get("target_name")
        # 包装成一个对象
        params = mute_target_vo.MuteTargetVo(target_type, target_code)
        # 实现 更新，创建
        return jsonify(web_service_impl.WebServericeImpl().mute_impl(params))
    else:
        return jsonify({"msg": "调用方式出错", "code": 400})


@app.route("/restart_all_mute_target", methods=['GET', 'POST'])
def restart_all_mute_target():
    '''
    将所有暂停标的策略重新开启，下一个交易日又可生效
    :return:
    '''
    if request.method == 'POST':
        return jsonify(web_service_impl.WebServericeImpl().restart_all_mute_target())
    else:
        return jsonify({"msg": "调用方式出错", "code": 400})

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True,threaded=True)
