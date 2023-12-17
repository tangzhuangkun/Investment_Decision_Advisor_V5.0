/* 预估所有指数的最新构成 */

/* 将之前指数的最新构成清空 */
truncate table financial_data.all_sources_index_latest_info;
truncate table financial_data.mix_top10_with_bottom_no_repeat;


insert into financial_data.all_sources_index_latest_info (index_code, index_name, stock_code, stock_name, stock_exchange_location,
       stock_market_code, weight, source, index_company, p_day)
/* 各个来源的指数的最新构成情况*/
select target.target_code, target.target_name, stock_code, stock_name, stock_exchange_location,
       stock_market_code, weight, a.source, index_company, a.p_day
from
(select index_code, index_name, stock_code, stock_name, stock_exchange_location,
       stock_market_code, weight, source, index_company, p_day
from financial_data.index_constituent_stocks_weight) a
inner join
/* 各个来源的指数的最新的业务日期*/
(select index_code, source, max(p_day) as p_day
from financial_data.index_constituent_stocks_weight
group by index_code, source)  m
on a.index_code = m.index_code
and a.source = m.source
and a.p_day = m.p_day
inner join
/* 当前标的池中的指数 */
(
select target_code, target_name
from target_pool.investment_target
where target_type = 'index'
and status = 'active'
and monitoring_frequency = 'daily'
) as target
on target.target_code = a.index_code
and target.target_code = m.index_code
group by target.target_code, target.target_name, stock_code, stock_name, stock_exchange_location,
       stock_market_code, weight, a.source, index_company, a.p_day
order by target.target_code, a.source, a.weight desc;


insert into financial_data.mix_top10_with_bottom_no_repeat (index_code, index_name, stock_code, stock_name, stock_exchange_location,
       stock_market_code, weight, source, index_company, p_day)
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
order by mix.index_code, raw.weight desc;
