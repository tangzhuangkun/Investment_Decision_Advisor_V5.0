/* 预估所有指数的最新构成 */

/* 将视图删除 */
drop view if exists all_sources_index_latest_info;
drop view if exists mix_top10_with_bottom;
truncate table mix_top10_with_bottom_no_repeat;

/* 创建视图*/
create view all_sources_index_latest_info as
/* 各个来源的指数的最新构成情况*/
select a.index_code, index_name, stock_code, stock_name, stock_exchange_location,
       stock_market_code, weight, a.source, index_company, a.p_day
from
(select index_code, index_name, stock_code, stock_name, stock_exchange_location,
       stock_market_code, weight, source, index_company, p_day
from index_constituent_stocks_weight) a
inner join
/* 各个来源的指数的最新的业务日期*/
(select index_code, source, max(p_day) as p_day
from index_constituent_stocks_weight
group by index_code, source)  m
on a.index_code = m.index_code
and a.source = m.source
and a.p_day = m.p_day
group by a.index_code, index_name, stock_code, stock_name, stock_exchange_location,
       stock_market_code, weight, a.source, index_company, a.p_day
order by a.index_code, a.source, a.weight desc;




/* 创建视图 */
create view mix_top10_with_bottom as
/* 拼接中证的最新前10权重股+每月中证指数文件中的10位之后的权重股+国证指数 */
select index_code, index_name, stock_code, stock_name, stock_exchange_location,
       stock_market_code, weight, source, index_company, p_day
from
/* 拼接中证的最新前十权重股 */
(select index_code, index_name, stock_code, stock_name, stock_exchange_location,
       stock_market_code, weight, source, index_company, p_day
from all_sources_index_latest_info
where source = '中证官网' and index_company = '中证'
union all
/* 取出视图中中证公司weight倒序，10名之后的 */
(select index_code, index_name, stock_code, stock_name, stock_exchange_location,
       stock_market_code, weight, source, index_company, p_day
 from all_sources_index_latest_info a
where
source = '中证权重文件'
and index_company= '中证'
and ( select count(1) from all_sources_index_latest_info b
				where b.index_company= '中证'
				and b.source = '中证权重文件'
				and a.index_code = b.index_code
				and a.weight<b.weight
				order by b.weight desc) >= 10
group by index_code, index_name, stock_code, stock_name, stock_exchange_location,
       stock_market_code, weight, source, index_company, p_day
order by a.index_code, a.weight desc)
union all
/* 拼接国证公司的指数信息 */
select index_code, index_name, stock_code, stock_name, stock_exchange_location,
       stock_market_code, weight, source, index_company, p_day
from all_sources_index_latest_info
where source = '国证官网' and index_company = '国证') cs_cn
order by index_code, source, weight desc, stock_code;


/* 插入表格中 */
/* 拼接中证的最新前10权重股+每月中证指数文件中的10位之后的权重股,去除重复股票+国证指数 */
insert into mix_top10_with_bottom_no_repeat (index_code, index_name, stock_code, stock_name, stock_exchange_location,
       stock_market_code, weight, source, index_company, p_day)
/* 如果视图（中证的最新前10权重股+每月中证指数文件中10位之后的权重股）中股票有重复，以最新日期的为准，忽略旧日期的，去除重复股票 */
select mt10wb.index_code, index_name, mt10wb.stock_code, mt10wb.stock_name, stock_exchange_location,
       stock_market_code, weight, source, index_company, mt10wb.p_day
from mix_top10_with_bottom mt10wb
inner join
/* 取出视图（中证的最新前10权重股+每月中证指数文件中的10位之后的权重股）中每个指数及成分股的最新日期 */
(select index_code, stock_code, stock_name, max(p_day) as p_day
from mix_top10_with_bottom
group by index_code, stock_code, stock_name
order by index_code, stock_code
) mm
on mt10wb.index_code = mm.index_code
and mt10wb.stock_code = mm.stock_code
and mt10wb.p_day = mm.p_day
group by mt10wb.index_code, index_name, mt10wb.stock_code, mt10wb.stock_name, stock_exchange_location,
       stock_market_code, weight, source, index_company, mt10wb.p_day
order by mt10wb.index_code, source, weight desc, mt10wb.stock_code;

