

class FundStrategyPEGEstimation:
    # 指数基金策略，PEG指标(市盈率相对盈利增长比率)
    # 用于衡量行业的估值与成长性
    # 频率：每个交易日，盘中

    def __init__(self):
        pass

    # PEG = PE / (归母公司净利润(TTM)增长率 * 100) # 如果 PE 或 增长率为负，则为 nan
    # PEG=  PE/（企业年盈利增长率*100）