import requests
import time
import json

import sys
sys.path.append("..")
import parsers.disguise as disguise
import log.custom_logger as custom_logger
import db_mapper.financial_data.chn_gov_bonds_rates_di_mapper as chn_gov_bonds_rates_di_mapper

class CollectCHNGovBondsRates:
    # 从中国债券信息网收集 中国债券到期收益率
    # 运行频率：每个交易日收盘后

    def __init__(self):
        # 获取当前时间
        self.today = time.strftime("%Y-%m-%d", time.localtime())

    def millisecond_to_time(self, millis):
        """13位时间戳转换为日期格式字符串"""
        # millis, 13位时间戳
        return time.strftime('%Y-%m-%d', time.localtime(millis / 1000))

    def call_bonds_interface_to_collect_all_historical_data(self, start_day, end_day, is_only_today=0):
        # 调用中国债券信息网接口,收集国债各到期品种的过往到期收益率

        # header，伪装的UA
        # proxy，伪装的IP
        # start_day, 开始日期， 如 2021-11-01
        # end_day, 结束日期， 如 2021-11-04（start_day和end_day不可相同）
        # is_only_today,是否只收集今天，默认 否（0），选是（1）

        bonds_interface_address = "https://yield.chinabond.com.cn/cbweb-mn/yc/queryYz?bjlx=no&&dcq=0.083333,1m;0.166667,2m;0.25,3m;0.5,6m;0.75,9m;1,1y;2,2y;3,3y;5,5y;7,7y;10,10y;&&startTime="+start_day+"&&endTime="+end_day+"&&qxlx=0,&&yqqxN=N&&yqqxK=K&&par=day&&ycDefIds=2c9081e50a2f9606010a3068cae70001,&&locale=zh_CN"

        # 解决报错 InsecureRequestWarning: Unverified HTTPS request is being made
        requests.packages.urllib3.disable_warnings()

        try:
            # 伪装，隐藏UA和IP
            header, proxy = disguise.Disguise().assemble_header_proxy()
            # 得到页面的信息
            raw_page = requests.post(bonds_interface_address, headers=header, proxies=proxy, verify=False, stream=False,
                                    timeout=10).text
        except Exception as e:
            # 日志记录
            msg = "从中国债券信息网" + bonds_interface_address + '  ' + "获取当日国债收益率失败,错误为 "+ str(e) +" 即将重试 "
            custom_logger.CustomLogger().log_writter(msg, lev='warning')
            return self.call_bonds_interface_to_collect_all_historical_data(start_day, end_day, is_only_today=0)

        # 转换成字典数据
        # [{"ycDefId":"2c9081e50a2f9606010a3068cae700015.0","ycDefName":"中债国债收益率曲线(到期)(5y)","ycYWName":null,"worktime":"","seriesData":[[1635868800000,2.7986],[1635955200000,2.7759]],"isPoint":false,"hyCurve":false,"point":false},{"ycDefId":"2c9081e50a2f9606010a3068cae7000110.0","ycDefName":"中债国债收益率曲线(到期)(10y)","ycYWName":null,"worktime":"","seriesData":[[1635868800000,2.9385],[1635955200000,2.9261]],"isPoint":false,"hyCurve":false,"point":false},{"ycDefId":"yzdcqx","ycDefName":"点差曲线","ycYWName":null,"worktime":null,"seriesData":[[1635868800000,0.1399],[1635955200000,0.1502]],"isPoint":false,"hyCurve":false,"point":false}]
        data_json_list = json.loads(raw_page)

        # 国债到期期限排序
        term_order_dict = {0:'1m', 1:'2m', 2:'3m', 3:'6m', 4:'9m', 5:'1y', 6:'2y', 7:'3y', 8:'5y', 9:'7y', 10:'10y'}

        # 遍历 国债各时间到期信息
        for i in range(len(data_json_list)):
            # 每组信息如下
            # {"ycDefId":"2c9081e50a2f9606010a3068cae700010.083333","ycDefName":"中债国债收益率曲线(到期)(1m)","ycYWName":null,"worktime":"","seriesData":[[1633622400000,1.776],[1633708800000,1.7659],,,],"isPoint":false,"hyCurve":false,"point":false}

            # 第一组到期信息，需要插入数据库
            if i == 0:
                # 优先插入最新日期的数据
                for j in range(len(data_json_list[i]["seriesData"]) - 1, -1+is_only_today, -1):
                    # 时间和利率信息如下
                    # [[1633622400000,1.776],[1633708800000,1.7659],,,]
                    trading_day = self.millisecond_to_time(data_json_list[i]["seriesData"][j][0])
                    rate = data_json_list[i]["seriesData"][j][1]
                    try:
                        # 插入的SQL
                        chn_gov_bonds_rates_di_mapper.ChnGovBondsRatesDiMapper().collect_bond_rate('1m', rate,trading_day,'中国债券信息网',self.today)
                    except Exception as e:
                        # 日志记录
                        msg = '收集国债到期收益率(1月期)， 插入 '+str(trading_day)+'的数据 失败' + '  ' + str(e)
                        custom_logger.CustomLogger().log_writter(msg, 'error')

            # 其它组到期信息，需要更新数据库
            else:
                # 优先更新最新日期的数据
                for j in range(len(data_json_list[i]["seriesData"]) - 1, -1+is_only_today, -1):
                    # 时间和利率信息如下
                    # [[1633622400000,1.776],[1633708800000,1.7659],,,]
                    trading_day = self.millisecond_to_time(data_json_list[i]["seriesData"][j][0])
                    rate = data_json_list[i]["seriesData"][j][1]
                    try:
                        # 更新的SQL
                        # updating_sql = "UPDATE chn_gov_bonds_rates_di SET "+ term_order_dict[i]+" = '%s' WHERE trading_day = '%s' AND source = '%s' " % (rate, trading_day, '中国债券信息网')
                        # db_operator.DBOperator().operate("update", "financial_data", updating_sql)
                        chn_gov_bonds_rates_di_mapper.ChnGovBondsRatesDiMapper().update_bond_info(term_order_dict[i], rate, trading_day, '中国债券信息网')

                    except Exception as e:
                        # 日志记录
                        msg = '更新国债到期收益率('+term_order_dict[i] +'期)， 插入 '+str(trading_day)+'的数据 失败' + '  ' + str(e)
                        custom_logger.CustomLogger().log_writter(msg, 'error')

    def main(self):
        # 从中国债券信息网收集 中国债券到期收益率主流程
        # 首先判断 中国国债到期收益率表 是否为空
        # 如果为空，从 2010-01-01 开始收集数据
        # 如果不为空，仅 数据库中的已收集的最新交易日至今的国债收益率数据

        try:
            # 获取总行数
            selecting_result = chn_gov_bonds_rates_di_mapper.ChnGovBondsRatesDiMapper().count_rows()
        except Exception as e:
            # 日志记录
            msg = "无法判断中国国债到期收益率表chn_gov_bonds_rates_di是否为空 " + '  ' + str(e)
            custom_logger.CustomLogger().log_writter(msg, 'error')
            return None

        # 如果表格为空，收集从 2010-01-01至今的数据
        if selecting_result["total_rows"] == 0:
            self.call_bonds_interface_to_collect_all_historical_data(start_day = "2010-01-01", end_day = self.today, is_only_today=0)
        else:
            # 获取中国国债到期收益率表chn_gov_bonds_rates_di已收集的最新交易日
            try:
                # 最新的日期
                selecting_max_date = chn_gov_bonds_rates_di_mapper.ChnGovBondsRatesDiMapper().max_date()
                self.call_bonds_interface_to_collect_all_historical_data(start_day=str(selecting_max_date["max_day"]),
                                                                         end_day=self.today, is_only_today=1)

            except Exception as e:
                # 日志记录
                msg = "无法从中国国债到期收益率表chn_gov_bonds_rates_di已收集的最新交易日信息 " + '  ' + str(e)
                custom_logger.CustomLogger().log_writter(msg, 'error')
                return None





if __name__ == '__main__':
    time_start = time.time()
    go = CollectCHNGovBondsRates()
    #go.call_bonds_interface_to_collect_all_historical_data("2021-11-04","2021-11-05",is_only_today=1)
    go.main()
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))