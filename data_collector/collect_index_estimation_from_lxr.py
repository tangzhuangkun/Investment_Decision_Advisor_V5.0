import datetime
import json
import sys
import time

import requests

sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger
import data_miner.data_miner_common_db_operator as data_miner_common_db_operator

class CollectIndexEstimationFromLXR:
    # 从理杏仁收集指数估值信息
    # dyr ：股息率
    # pe_ttm：动态市盈率
    # pb ：市净率
    # ps_ttm ：动态市销率
    # # 运行频率：每个交易日收盘后

    def __init__(self):

        # 要从理杏仁采集估值的指数
        self.index_code_name_dict = { "1000002":"沪深A股","000300":"沪深300"}
        # 获取当前时间
        self.today = time.strftime("%Y-%m-%d", time.localtime())


    def collect_index_estimation_in_a_period_time(self, start_date, end_date):
        # 调取理杏仁接口，获取一段时间范围内，指数估值数据, 并储存
        # param:  index_code, 指数代码，如 000300
        # param:  start_date, 开始时间，如 2020-11-12
        # param:  end_date, 结束时间，默认为空，如 2020-11-13
        # 输出： 将获取到指数估值数据存入数据库

        # 随机获取一个token
        token = data_miner_common_db_operator.DataMinerCommonDBOperator().get_one_token("lxr")
        # 理杏仁要求 在请求的headers里面设置Content-Type为application/json。
        headers = {'Content-Type': 'application/json'}
        # 理杏仁 获取A股指数基本面数据 接口，文档如下
        # https://www.lixinger.com/open/api/doc?api-key=a%2Findex%2Ffundamental
        url = 'https://open.lixinger.com/api/a/index/fundamental'

        # 接口参数，
        # dyr：股息率
        # pe_ttm ： 滚动市盈率
        # pb ： 市净率
        # ps_ttm ： 滚动市销率

        # 估值方式
        # mcw ： 市值加权 ， 以PE-TTM为例，所有样品公司市值之和 / 所有样品公司归属于母公司净利润之和
        # ew ： 等权， 以PE-TTM为例，算出所有公司的PE-TTM，然后通过(n / ∑(1 / PE.i))计算出来
        # ewpvo ： 等权， 当计算PE-TTM的时候，意味着剔除所有不赚钱的企业。
        #                当计算PB的时候，意味着剔除所有净资产为负数的企业（多见于ST或者快退市的企业，港股和美股有部分长期大比率分红而导致净资产为负数的企业）。
        #                当计算PS-TTM的时候，意味着剔除所有营业额为0的企业（可见于极少部分即将退市的企业，以及少部分港股的投资公司）。
        #                当计算股息率的时候，意味着剔除所有不分红的企业。
        # avg ： 平均值， 以PE-TTM为例，算出所有样品公司的滚动市盈率，剔除负数，然后使用四分位距（interquartile range, IQR）去除极端值，然后加和求平均值
        # median ： 中位数， 以PE-TTM为例，算出所有样品公司的市盈率，然后排序，然后取最中间的那个数；如果是偶数，那么取中间的两个，加和求半。

        # 遍历要采集估值的指数代码
        for index_code in self.index_code_name_dict.keys():
            parms = {"token": token,
                     "startDate": start_date,
                     "endDate": end_date,
                     "stockCodes":
                         [
                             index_code
                         ],
                     "metricsList": [
                        "pe_ttm.mcw",
                        "pe_ttm.ew",
                        "pe_ttm.ewpvo",
                        "pe_ttm.avg",
                        "pe_ttm.median",
                        "pb.mcw",
                        "pb.ew",
                        "pb.ewpvo",
                        "pb.avg",
                        "pb.median",
                        "ps_ttm.mcw",
                        "ps_ttm.ew",
                        "ps_ttm.ewpvo",
                        "ps_ttm.avg",
                        "ps_ttm.median",
                        "dyr.mcw",
                        "dyr.ew",
                        "dyr.ewpvo",
                        "dyr.avg",
                        "dyr.median"
                     ]}

            values = json.dumps(parms)
            # 调用理杏仁接口
            req = requests.post(url, data=values, headers=headers)
            content = req.json()

            if 'error' in content and content.get('error').get('message') == "Illegal token.":
                # 日志记录失败
                msg = '无法使用理杏仁token ' + token + ' ' + '来采集指数估值 ' + \
                      index_code+ '' +self.index_code_name_dict.get(index_code) + ' ' + start_date + ' ' + end_date \
                      + ' 报错token为 ' + token
                custom_logger.CustomLogger().log_writter(msg, 'error')
                return self.collect_index_estimation_in_a_period_time(index_code, start_date, end_date)

            try:
                # 数据存入数据库
                self.save_content_into_db(content)
            except Exception as e:
                # 日志记录失败
                msg = '数据存入数据库失败。 ' + '理杏仁指数估值接口返回为 '+str(content) + '。 抛错为 '+ str(e) + \
                      ' 使用的Token为' + token
                custom_logger.CustomLogger().log_writter(msg, 'error')


    def collect_index_estimation_in_a_special_date(self, date):
        # 调取理杏仁接口，获取某一个具体日期，指数估值数据, 并储存
        # param:  index_code, 指数代码，如 000300
        # param:  date, 日期，如 2020-11-12
        # 输出： 将获取到指数估值数据存入数据库

        # 随机获取一个token
        token = data_miner_common_db_operator.DataMinerCommonDBOperator().get_one_token("lxr")
        # 理杏仁要求 在请求的headers里面设置Content-Type为application/json。
        headers = {'Content-Type': 'application/json'}
        # 理杏仁 获取A股指数基本面数据 接口，文档如下
        # https://www.lixinger.com/open/api/doc?api-key=a%2Findex%2Ffundamental
        url = 'https://open.lixinger.com/api/a/index/fundamental'

        # 接口参数，
        # dyr：股息率
        # pe_ttm ： 滚动市盈率
        # pb ： 市净率
        # ps_ttm ： 滚动市销率

        # 估值方式
        # mcw ： 市值加权 ， 以PE-TTM为例，所有样品公司市值之和 / 所有样品公司归属于母公司净利润之和
        # ew ： 等权， 以PE-TTM为例，算出所有公司的PE-TTM，然后通过(n / ∑(1 / PE.i))计算出来
        # ewpvo ： 等权， 当计算PE-TTM的时候，意味着剔除所有不赚钱的企业。
        #                当计算PB的时候，意味着剔除所有净资产为负数的企业（多见于ST或者快退市的企业，港股和美股有部分长期大比率分红而导致净资产为负数的企业）。
        #                当计算PS-TTM的时候，意味着剔除所有营业额为0的企业（可见于极少部分即将退市的企业，以及少部分港股的投资公司）。
        #                当计算股息率的时候，意味着剔除所有不分红的企业。
        # avg ： 平均值， 以PE-TTM为例，算出所有样品公司的滚动市盈率，剔除负数，然后使用四分位距（interquartile range, IQR）去除极端值，然后加和求平均值
        # median ： 中位数， 以PE-TTM为例，算出所有样品公司的市盈率，然后排序，然后取最中间的那个数；如果是偶数，那么取中间的两个，加和求半。

        # 取出指数代码
        index_code_list = list(self.index_code_name_dict.keys())

        parms = {"token": token,
                 "date": date,
                 "stockCodes":
                     index_code_list,
                 "metricsList": [
                     "pe_ttm.mcw",
                     "pe_ttm.ew",
                     "pe_ttm.ewpvo",
                     "pe_ttm.avg",
                     "pe_ttm.median",
                     "pb.mcw",
                     "pb.ew",
                     "pb.ewpvo",
                     "pb.avg",
                     "pb.median",
                     "ps_ttm.mcw",
                     "ps_ttm.ew",
                     "ps_ttm.ewpvo",
                     "ps_ttm.avg",
                     "ps_ttm.median",
                     "dyr.mcw",
                     "dyr.ew",
                     "dyr.ewpvo",
                     "dyr.avg",
                     "dyr.median"
                 ]}

        values = json.dumps(parms)
        # 调用理杏仁接口
        req = requests.post(url, data=values, headers=headers)
        content = req.json()

        if 'error' in content and content.get('error').get('message') == "Illegal token.":
            # 日志记录失败
            msg = '无法使用理杏仁token ' + token + ' ' + '来采集指数估值 ' + \
                  str(self.index_code_name_dict) + ' ' + date + ' 报错token为 ' + token
            custom_logger.CustomLogger().log_writter(msg, 'error')
            return self.collect_index_estimation_in_a_special_date(date)

        try:
            # 数据存入数据库
            self.save_content_into_db(content)
        except Exception as e:
            # 日志记录失败
            msg = '数据存入数据库失败。 ' + '理杏仁指数估值接口返回为 '+str(content) + '。 抛错为 '+ str(e) \
                  + ' 使用的Token为' + token
            custom_logger.CustomLogger().log_writter(msg, 'error')


    def save_content_into_db(self,content):
        # 将 理杏仁接口返回的数据 存入数据库
        # param:  content, 理杏仁接口返回的数据
        # param： range， 时间范围，只能填 period 或者 date
        # return: 将数据存入数据库

        # 解析返回的数据
        # piece 如
        '''
        {'date': '2021-11-05T00:00:00+08:00',
         'dyr':
             {'avg': 0.018305249961631118,
              'ew': 0.018305249961631118,
              'ewpvo': 0.019825180463860417,
              'mcw': 0.02368823582947087,
              'median': 0.010602775046762337},
         'pb':
             {'avg': 4.424412802047905,
              'ew': 1.8671774939979564,
              'ewpvo': 1.8671774939979564,
              'mcw': 1.5625370472303437,
              'median': 3.008043580249194},
         'pe_ttm':
             {'avg': 33.23077720565601,
              'ew': 18.807741263693835,
              'ewpvo': 14.946634171935743,
              'mcw': 12.979291671999727,
              'median': 27.7841987696509},
         'ps_ttm':
             {'avg': 4.946294316967682,
              'ew': 1.23027246244871,
              'ewpvo': 1.23027246244871,
              'mcw': 1.3616166194044408,
              'median': 3.1671710111824245},
         'stockCode': '000300'}
        '''

        for piece in content["data"]:
            index_code = piece['stockCode']
            index_name = self.index_code_name_dict[index_code]
            trading_date = piece['date'][:10]


            pe_ttm_mcw = piece['pe_ttm']['mcw']
            pe_ttm_ew = piece['pe_ttm']['ew']
            pe_ttm_ewpvo = piece['pe_ttm']['ewpvo']
            pe_ttm_avg = piece['pe_ttm']['avg']
            pe_ttm_median = piece['pe_ttm']['median']

            pb_mcw = piece['pb']['mcw']
            pb_ew = piece['pb']['ew']
            pb_ewpvo = piece['pb']['ewpvo']
            pb_avg = piece['pb']['avg']
            pb_median = piece['pb']['median']

            ps_ttm_mcw = piece['ps_ttm']['mcw']
            ps_ttm_ew = piece['ps_ttm']['ew']
            ps_ttm_ewpvo = piece['ps_ttm']['ewpvo']
            ps_ttm_avg = piece['ps_ttm']['avg']
            ps_ttm_median = piece['ps_ttm']['median']

            dyr_mcw = piece['dyr']['mcw']
            dyr_ew = piece['dyr']['ew']
            dyr_ewpvo = piece['dyr']['ewpvo']
            dyr_avg = piece['dyr']['avg']
            dyr_median = piece['dyr']['median']

            # 存入数据库
            inserting_sql = "INSERT INTO index_estimation_from_lxr_di (index_code, index_name, trading_date,pe_ttm_mcw,pe_ttm_ew,pe_ttm_ewpvo,pe_ttm_avg,pe_ttm_median,pb_mcw,pb_ew,pb_ewpvo,pb_avg,pb_median,ps_ttm_mcw,ps_ttm_ew,ps_ttm_ewpvo,ps_ttm_avg,ps_ttm_median,dyr_mcw,dyr_ew,dyr_ewpvo,dyr_avg,dyr_median,source,submission_date) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s' )" % (index_code,index_name,trading_date,pe_ttm_mcw,pe_ttm_ew,pe_ttm_ewpvo,pe_ttm_avg,pe_ttm_median,pb_mcw,pb_ew,pb_ewpvo,pb_avg,pb_median,ps_ttm_mcw,ps_ttm_ew,ps_ttm_ewpvo,ps_ttm_avg,ps_ttm_median,dyr_mcw,dyr_ew,dyr_ewpvo,dyr_avg,dyr_median,'理杏仁',self.today)
            db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)


    def main(self):


        try:
            # 查询sql
            selecting_sql = "SELECT COUNT(*) as total_rows FROM index_estimation_from_lxr_di"
            # 查询
            selecting_result = db_operator.DBOperator().select_one("financial_data", selecting_sql)

        except Exception as e:
            # 日志记录
            msg = "无法判断理杏仁的指数估值信息表 index_estimation_from_lxr_di是否为空 " + '  ' + str(e)
            custom_logger.CustomLogger().log_writter(msg, 'error')
            return None

        # 如果表格为空，收集从 2010-01-01至今的数据
        if selecting_result["total_rows"] == 0:
            self.collect_index_estimation_in_a_period_time(start_date = "2010-01-01", end_date = self.today)
        else:
            # 获取 理杏仁的指数估值信息表 index_estimation_from_lxr_di 已收集的最新交易日
            try:
                # 查询sql
                selecting_max_date_sql = "SELECT max(trading_date) max_day FROM index_estimation_from_lxr_di"
                # 查询
                selecting_max_date = db_operator.DBOperator().select_one("financial_data", selecting_max_date_sql)

                # 数据库中的已收集的最大日期
                max_trading_day = selecting_max_date["max_day"]
                # 今天的日期
                current_day = datetime.datetime.today().date()
                # 相差天数
                day_diff = (current_day - max_trading_day).days

                # 日期相差1天及以下
                if( day_diff<=1):
                    self.collect_index_estimation_in_a_special_date(self.today)
                # 日期相差2天及以上
                else:
                    self.collect_index_estimation_in_a_period_time(start_date = str(selecting_max_date["max_day"]+datetime.timedelta(days=1)), end_date = self.today)


            except Exception as e:
                # 日志记录
                msg = "无法从理杏仁的指数估值信息表 index_estimation_from_lxr_di已收集的最新交易日信息 " + '  ' + str(e)
                custom_logger.CustomLogger().log_writter(msg, 'error')
                return None




if __name__ == '__main__':
    time_start = time.time()
    go = CollectIndexEstimationFromLXR()
    #go.collect_index_estimation_in_a_period_time("2021-11-01","2021-11-05")
    #go.collect_index_estimation_in_a_special_date("2021-11-05")
    go.main()
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))