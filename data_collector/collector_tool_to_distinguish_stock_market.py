import time


class CollectorToolToDistinguishStockMarket:
    # 识别股票代码的交易所

    def __init__(self):
        pass

    def distinguishStockMarketByCode(self,stock_code):
        # 通过股票代码获取股票交易所
        # stock_code, 股票代码，大陆市场的股票市场

        '''
        沪市A股主板代码：600、601、603、605打头；B股买卖的代码是以900打头；
        沪市科创板股票代码：688开头。

        深市A股主板代码：000打头；B股买卖的代码是以200打头；
        深市中小板股票代码：002打头；
        深市创业板股票代码：300打头；


        配股：
        沪市以700打头，
        深市以080打头；

        权证：
        沪市以580打头，
        深市以031打头；


        :param stock_code:
        :return:
        '''

        # 判断是否为沪市
        if stock_code.startswith('6') or stock_code.startswith('900') or stock_code.startswith('700') or stock_code.startswith('580'):
            return ('sh', 'XSHG')
        # 判断是否为深市
        elif stock_code.startswith('0') or stock_code.startswith('2') or stock_code.startswith('3'):
            return ('sz', 'XSHE')
        else:
            return ('UNKOWN', 'UNKOWN')



if __name__ == '__main__':
    time_start = time.time()
    go = CollectorToolToDistinguishStockMarket()
    stock_market_code = go.distinguishStockMarketByCode('002142')
    print(stock_market_code)
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))