/* 计算每一天的股债收益比 */
insert into aggregated_data.stock_bond_ratio_di (index_code, index_name, trading_date, index_tv, index_ta, index_cp,
                                                 index_cpc, pe, stock_yield_rate, 10y_bond_rate, ratio)
select index_code,
       index_name,
       trading_date,
       index_tv,
       index_ta,
       index_cp,
       index_cpc,
       pe,
       /*  使用cast代替round，解决精度问题 */
       cast(1/pe as decimal(9,6))             as stock_yield_rate,
       cast(10y/100 as decimal(9,6))                    as bond_rate,
       cast(cast(1/pe as decimal(9,6)) / cast(10y/100 as decimal(9,6)) as decimal(20,17)) as ratio
from (
    /*  使用沪深300市值加权的市盈率作为参考 */
      (select index_code, index_name, pe_ttm_mcw as pe, trading_date, tv as index_tv, ta as index_ta, cp as index_cp, cpc as index_cpc
      from index_estimation_from_lxr_di
      where index_code = '000300'
      order by trading_date desc) a
         join
     /* 获取 10年期国债收益率 */
         (select 10y, trading_day
          from chn_gov_bonds_rates_di cgbrd
          order by trading_day desc) b
     on a.trading_date = b.trading_day)
order by a.trading_date desc;