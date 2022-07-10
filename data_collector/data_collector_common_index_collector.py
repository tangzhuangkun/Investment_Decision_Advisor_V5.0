import requests

import decimal
import time
import sys
sys.path.append("..")
import parsers.disguise as disguise

class DataCollectorCommonIndexCollector:
    # 一些通用的，用于收集指数/股票某些属性的方法

    def __init__(self):
        pass


    """
    def get_target_latest_increasement_decreasement_rate(self, code_with_location):
        '''
        获取指数/股票最新的涨跌率
        :param code_with_location: 指数/股票代码(含上市地), 必须如 sz399965，代码前面带上市地
        :return: 最新的涨跌幅, 如 0.39% 即返回为 0.39
        '''

        # 伪装，隐藏UA和IP
        header, proxy = disguise.Disguise().assemble_header_proxy()

        # 接口地址
        # 接口返回: 指数名称，当前点数，当前价格，涨跌, 涨跌率，成交量（手），成交额（万元）, 总市值；
        # 接口返回如： 涨跌率, -0.86;
        # 只取 涨跌率
        url = 'https://qt.gtimg.cn/q=s_'+code_with_location
        content = requests.get(url, headers=header, proxies=proxy, verify=False)
        content_split = content.text.split('~')
        return decimal.Decimal(content_split[5])
    """

    def get_target_latest_increasement_decreasement_rate(self, code_with_location):
        '''
        获取指数/股票最新的涨跌率
        :param code_with_location: 指数/股票代码(含上市地), 必须如 sz399965，代码前面带上市地
        :return: 最新的涨跌幅, 如 0.39% 即返回为 0.39
        '''

        # 接口地址
        # 接口返回: 指数名称，当前点数，当前价格，涨跌, 涨跌率，成交量（手），成交额（万元）, 总市值；
        # 接口返回如： 涨跌率, -0.86;
        # 只取 涨跌率
        url = 'https://qt.gtimg.cn/q=s_' + code_with_location
        requests.packages.urllib3.disable_warnings()
        content = requests.get(url, verify=False)
        content_split = content.text.split('~')
        return decimal.Decimal(content_split[5])





if __name__ == '__main__':
    time_start = time.time()
    go = DataCollectorCommonIndexCollector()
    result = go.get_target_latest_increasement_decreasement_rate('sz399965')
    print(result)
    time_end = time.time()
    print('time:')
    print(time_end - time_start)