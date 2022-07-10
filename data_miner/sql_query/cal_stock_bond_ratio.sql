/* 计算每一天的股债收益比 */
insert into aggregated_data.stock_bond_ratio_di (index_code, index_name, trading_date, pe, stock_yield_rate,
                                                 10y_bond_rate, ratio)
select index_code,
       index_name,
       trading_date,
       pe,
       round(1 / pe, 6)                       as stock_yield_rate,
       round(10y / 100, 6)                    as bond_rate,
       round(1 / pe, 6) / round(10y / 100, 6) as ratio
from (
    /*  使用沪深300市值加权的市盈率作为参考 */
      (select index_code, index_name, pe_ttm_mcw as pe, trading_date
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