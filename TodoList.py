
# TODO 修正log文件，解决线上每日运行日志时有时无问题，到了01：01，自行创建一个新的日记log file
# TODO 日志文件日期依然是错乱的 2022-07-18 21：36

# TODO 理杏仁的token因为会定期改变，不再写死在程序配置文件中，改为从数据库获取
#  -- done，2022-07-10 21：30

# TODO 删除线上数据库中的无用库及表
#  --done

# TODO mysql 跑一下，每天的市盈率涨跌率 与 真实沪深300的涨跌率 方差 对比
# --done

# TODO 从理杏仁获取数据时间跨度不得超过10年问题--done

# TODO  所有用到 db_operator.DBOperator().operate地方，需要修正一下 ， 参考 web_service_impl.py中的应用

# TODO 从akshare获取指数行情数据

# TODO 回测，从沪深300指数中获取低估值股票的策略

#TODO 升级日志组件，使其更灵活

# TODO 日志有打印重复问题，debug时却又好了，当前至少存在两处问题，搜索"此日志被多次触发，需要排查"

# TODO 股息率策略

# TODO 修正index_components_historical_estimations的mapper

# TODO 检查数据表，mix_top10_with_bottom_no_repeat，每天的数据是否为最新  --done

# TODO 紧急，修正 mix_top10_with_bottom_no_repeat，按以下  --done
"""
/*预测指数的最新构成信息*/
select mix.index_code, raw.index_name, mix.stock_code, raw.stock_name, raw.stock_exchange_location, raw.stock_market_code, raw.weight, raw.source, raw.index_company, mix.p_day
from
/* 获取最新日期，指数及其股票代码 */
(select index_code, stock_code, max(p_day) as p_day
from financial_data.all_sources_index_latest_info
group by index_code, stock_code) as mix
left join
financial_data.all_sources_index_latest_info as raw
on mix.index_code = raw.index_code
and mix.stock_code = raw.stock_code
and mix.p_day = raw.p_day
group by mix.index_code, raw.index_name, mix.stock_code, raw.stock_name, raw.stock_exchange_location, raw.stock_market_code, raw.weight, raw.source, raw.index_company, mix.p_day
order by raw.weight desc
"""

# todo 收集 中证衍生指数，净收益，全收益 -- done

# TODO 验证多线程下，中证数据是否收集正确 -- done

# TODO 验证多线程，单线程下，中证数据是否收集一致，目前已有多线程 -- done

# TODO 中证指数收集，在satisfied_index_list之后，为什么会跳变，国证是否也有同样情况 -- done

# ToDo 单线程，cs, H30404, H50046 收集失败 -- done

# TODO 使用akshare 替换 理杏仁 获取数据

# todo 获取 中证红利低波动100指数 (930955) 的涨跌幅

# TODO 股票策略，每天仅提醒一次

# TODO 需要收集个股指标，
"""
当日收盘价(元)	
当日涨跌幅(%)	
总市值(元)	
流通市值(元)	
总股本(股)	
流通股本	
PE(TTM)	
PE(扣非)	
市净率	
市现率(TTM)	
市销率(TTM)	
股息率
股息率ttm
PEG值（TTM）
PEG值（扣非TTM）
"""

#todo 收集步骤
"""
1、通过  https://data.eastmoney.com/gzfx/list.html， 收集全部A股标的股票的估值数据，当日收盘价(元)	当日涨跌幅(%)	总市值(元)	流通市值(元)	总股本(股)	流通股本	PE(TTM)	PE(静)	市净率	PEG值	市现率(TTM)	市销率(TTM)；

2、通过 https://data.eastmoney.com/gzfx/detail/600519.html， 收集 个股历史 的估值数据，当日收盘价(元)	当日涨跌幅(%)	总市值(元)	流通市值(元)	总股本(股)	流通股本	PE(TTM)	PE(静)	市净率	PEG值	市现率(TTM)	市销率(TTM)；

3、通过 AKSHARE stock_a_indicator_lg， 获取 股息率，股息率ttm；

4、通过 AKSHARE stock_financial_analysis_indicator（https://vip.stock.finance.sina.com.cn/corp/go.php/vFD_FinanceSummary/stockid/600004.phtml）, 新浪财经-财务分析-财务指标， 公告日期前，加权平均净资产收益率 和 扣非净利润， 

5、计算，净利润TTM, 扣非净利润TTM， 计算 PE(扣非)	， 填补 估值数据


"""

#todo https://data.eastmoney.com/gzfx/detail/600519.html 丰富数据源， 当日收盘价(元)	当日涨跌幅(%)	总市值(元)	流通市值(元)	总股本(股)	流通股本	PE(TTM)	PE(静)	市净率	PEG值	市现率(TTM)	市销率(TTM)

# TODO AKSHARE  stock_financial_abstract_ths, 使用 扣非净利润计算 扣非市盈率

# ToDo AKSHARE stock_a_indicator_lg,  乐咕乐股-A 股个股指标: 市盈率, 市净率, 股息率ttm

# todo AKSHARE stock_financial_abstract， 同花顺-财务指标-主要指标

# todo AKSHARE stock_financial_analysis_indicator, 新浪财经-财务分析-财务指标