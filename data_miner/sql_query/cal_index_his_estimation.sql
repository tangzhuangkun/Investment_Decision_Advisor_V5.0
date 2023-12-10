/* 利用mysql，基于指数最新的成分股进行计算历史估值信息 */
/* 将计算结果插入另外一个的表中 */
insert into aggregated_data.index_components_historical_estimations (index_code, index_name, historical_date,
                                                                     pe_ttm, pe_ttm_effective_weight, pe_ttm_estimation_overall,
                                                                     pe_ttm_nonrecurring, pe_ttm_nonrecurring_effective_weight, pe_ttm_nonrecurring_estimation_overall,
                                                                     pb, pb_effective_weight, pb_estimation_overall,
                                                                     pb_wo_gw, pb_wo_gw_effective_weight, pb_wo_gw_estimation_overall,
                                                                     ps_ttm, ps_ttm_effective_weight, ps_ttm_estimation_overall,
                                                                     pcf_ttm, pcf_ttm_effective_weight, pcf_ttm_estimation_overall,
                                                                     dividend_yield, dividend_yield_effective_weight, dividend_yield_estimation_overall)

select   his.index_code, his.index_name, his.historical_date,
         his.pe_ttm, his.pe_ttm_effective_weight, round(his.pe_ttm*100/his.pe_ttm_effective_weight, 5) as pe_ttm_estimation_overall,
         pe_ttm_nonrecurring, pe_ttm_nonrecurring_effective_weight, round(his.pe_ttm_nonrecurring*100/his.pe_ttm_nonrecurring_effective_weight, 5) as pe_ttm_nonrecurring_estimation_overall,
         pb, pb_effective_weight, round(his.pb*100/his.pb_effective_weight, 5) as pb_estimation_overall,
         pb_wo_gw, pb_wo_gw_effective_weight, round(his.pb_wo_gw*100/his.pb_wo_gw_effective_weight, 5) as pb_wo_gw_estimation_overall,
         ps_ttm, ps_ttm_effective_weight, round(his.ps_ttm*100/his.ps_ttm_effective_weight, 5) as ps_ttm_estimation_overall,
         pcf_ttm, pcf_ttm_effective_weight, round(his.pcf_ttm*100/his.pcf_ttm_effective_weight, 5) as pcf_ttm_estimation_overall,
         dividend_yield, dividend_yield_effective_weight, round(his.dividend_yield*100/his.dividend_yield_effective_weight, 5) as dividend_yield_estimation_overall
from
(select a.index_code, a.index_name, c.date as historical_date,
/* 计算指数pe_ttm */
round(sum(
case
		when pe_ttm < 0 then  0
	else
		weight*pe_ttm/100
	end
),5) as pe_ttm,
/* 计算指数pe_ttm的有效权重，舍弃pe_ttm<=0的成份股权重 */
round(sum(
	case
		when pe_ttm < 0 then  0
	else
		weight
	end ),5) as pe_ttm_effective_weight,
/* 计算指数pe_ttm_nonrecurring */
round(sum(
case
		when pe_ttm_nonrecurring < 0 then  0
	else
		weight*pe_ttm_nonrecurring/100
	end
),5) as pe_ttm_nonrecurring,
/* 计算指数pe_ttm_nonrecurring的有效权重，舍弃pe_ttm_nonrecurring<=0的成份股权重 */
round(sum(
	case
		when pe_ttm_nonrecurring < 0 then  0
	else
		weight
	end ),5) as pe_ttm_nonrecurring_effective_weight,
/* 计算指数pb */
round(sum(
case
		when pb < 0 then  0
	else
		weight*pb/100
	end
),5) as pb,
/* 计算指数pb的有效权重，舍弃pb<=0的成份股权重 */
round(sum(
	case
		when pb < 0 then  0
	else
		weight
	end ),5) as pb_effective_weight,
/* 计算指数pb_wo_gw */
round(sum(
case
		when pb_wo_gw < 0 then  0
	else
		weight*pb_wo_gw/100
	end
),5) as pb_wo_gw,
/* 计算指数pb_wo_gw的有效权重，舍弃pb_wo_gw<=0的成份股权重 */
round(sum(
	case
		when pb_wo_gw < 0 then  0
	else
		weight
	end ),5) as pb_wo_gw_effective_weight,
/* 计算指数ps_ttm */
round(sum(
case
		when ps_ttm < 0 then  0
	else
		weight*ps_ttm/100
	end
),5) as ps_ttm,
/* 计算指数ps_ttm的有效权重，舍弃ps_ttm<=0的成份股权重 */
round(sum(
	case
		when ps_ttm < 0 then  0
	else
		weight
	end ),5) as ps_ttm_effective_weight,
/* 计算指数pcf_ttm */
round(sum(
case
		when pcf_ttm < 0 then  0
	else
		weight*pcf_ttm/100
	end
),5) as pcf_ttm,
/* 计算指数pcf_ttm的有效权重，舍弃pcf_ttm<=0的成份股权重 */
round(sum(
	case
		when pcf_ttm < 0 then  0
	else
		weight
	end ),5) as pcf_ttm_effective_weight,
/* 计算指数dividend_yield */
round(sum(
case
		when dividend_yield< 0 then  0
	else
		weight*dividend_yield/100
	end
),5) as dividend_yield,
/* 计算指数dividend_yield的有效权重，舍弃dividend_yield<=0的成份股权重 */
round(sum(
	case
		when dividend_yield< 0 then  0
	else
		weight
	end ),5) as dividend_yield_effective_weight
from
/* 获取指数及其成分股，权重，日期 */
financial_data.mix_top10_with_bottom_no_repeat a
inner join
/* 获取每个股票的历史指标信息 */
(select stock_code, stock_name, date, pe_ttm, pe_ttm_nonrecurring, pb, pb_wo_gw, ps_ttm, pcf_ttm, dividend_yield
from financial_data.stocks_main_estimation_indexes_historical_data) c
on a.stock_code = c.stock_code
group by c.date,a.index_code, a.index_name
order by c.date desc) as his;