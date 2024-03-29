#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

'''
# TODO
股票监控策略，处于绝对低估位置的行业龙头
满足条件：
1、沪深300成分股
2、公认的行业龙头，https://xueqiu.com/8132581807/216258901
3、有记录的交易日数据超过1500天--2500天，即至少超过6.17年--10年的上市时间，仅看10年以内的指标（每年约243个交易日）
4、涨幅超过x%，就抛出进行轮动，坚守纪律性；这个指标需要回测来判断；
5、实时PB, 实时PE, 实时扣非PE（当时涨跌幅*昨日扣非PE）处于处于历史最低水平，即 拉通历史来看，处于倒数5位以内
'''


'''
-- 统计每年平均有多少个交易日
select avg(totoal_trading_days) from
(
select trading_year,count(*) as totoal_trading_days from
(select substr(trading_date,1,4) as trading_year
from trading_days
where trading_date<'2022-01-01'
) as raw
group by trading_year) raw_two

2005	242
2006	241
2007	242
2008	246
2009	244
2010	242
2011	244
2012	243
2013	238
2014	245
2015	244
2016	244
2017	244
2018	243
2019	244
2020	243
2021	243

'''



'''
-- 获取股票，股票名称，最新日期，市净率，市净率低估排名，共有该股票多少交易日记录，当前市净率位于历史百分比位置
select raw.stock_code, raw.stock_name, raw.date, raw.pb, raw.row_num, record.total_record, raw.percent_num from 
(select stock_code, stock_name, date, pb,
row_number() OVER (partition by stock_code ORDER BY pb asc) AS row_num,  
percent_rank() OVER (partition by stock_code ORDER BY pb asc) AS percent_num
from stocks_main_estimation_indexes_historical_data) raw
left join 
(select stock_code, count(1) as total_record from stocks_main_estimation_indexes_historical_data group by stock_code) as record
on raw.stock_code = record.stock_code
where raw.date = (select max(date) from stocks_main_estimation_indexes_historical_data)
and record.total_record>1500
order by raw.percent_num asc;

-- 效果如 
600201	生物股份	2022-04-07	2.4402423138242970	1	2944	0
603866	桃李面包	2022-04-07	3.8300289940574705	3	1528	0.0013097576948264572
600000	浦发银行	2022-04-07	0.4308040872445288	18	2929	0.005806010928961749
601818	光大银行	2022-04-07	0.4847183905791494	18	2805	0.006062767475035664
600016	民生银行	2022-04-07	0.3489460092537104	23	2967	0.007417397167902899
601155	新城控股	2022-04-07	1.2309878673186585	18	1529	0.01112565445026178
600015	华夏银行	2022-04-07	0.3771476418174968	64	2957	0.02131258457374831
300146	汤臣倍健	2022-04-07	3.4025785285744368	57	2574	0.021764477263894286
601398	工商银行	2022-04-07	0.5916731966013162	66	2963	0.021944632005401754
000002	万科A	2022-04-07	1.0302332587489198	74	2821	0.025886524822695035
601939	建设银行	2022-04-07	0.6279756638616979	78	2964	0.025987175160310495
600597	光明乳业	2022-04-07	1.9467722644235010	86	2891	0.029411764705882353
000869	张裕Ａ	2022-04-07	1.8226508997219684	89	2961	0.02972972972972973
601288	农业银行	2022-04-07	0.5263202443663337	87	2844	0.030249736194161096
000961	中南建设	2022-04-07	0.5815674850847055	90	2937	0.030313351498637602
000671	阳光城	2022-04-07	0.5470418664477901	97	2902	0.033092037228541885
002146	荣盛发展	2022-04-07	0.4078522781617524	101	2934	0.03409478349812479
600648	外高桥	2022-04-07	1.3471860708200998	102	2928	0.03450632046463956
000656	金科股份	2022-04-07	0.7365313814048178	102	2893	0.03492392807745505

'''


'''
select raw.stock_code, raw.stock_name, raw.date, raw.pb, raw.share_price, raw.bc_rights, raw.row_num, record.total_record, raw.percent_num from 
(select stock_code, stock_name, date, pb, share_price, bc_rights,
row_number() OVER (partition by stock_code ORDER BY pb asc) AS row_num,  
percent_rank() OVER (partition by stock_code ORDER BY pb asc) AS percent_num
from stocks_main_estimation_indexes_historical_data) raw
left join 
(select stock_code, count(1) as total_record from stocks_main_estimation_indexes_historical_data group by stock_code) as record
on raw.stock_code = record.stock_code
left join (select distinct stock_code, date from stocks_main_estimation_indexes_historical_data where date>"2020-01-01") time
on raw.stock_code = time.stock_code 
where raw.date = time.date
and record.total_record>1500
and raw.row_num <=5
order by raw.percent_num asc

stock_code	stock_name	date	pb	share_price	bc_rights	row_num	total_record	percent_num
000002	万科A	2022-03-15	0.7558804373711309000000	14.800000	21.9117151000000000000000	1	2829	0
000031	大悦城	2021-11-29	0.7207347339649198000000	3.320000	3.8999999999999995000000	1	2790	0
000069	华侨城A	2022-03-15	0.6165856262811620000000	5.840000	17.8754940340000030000000	1	2934	0
000402	金融街	2022-03-15	0.4067713529958032500000	4.980000	9.8856000000000000000000	1	2977	0
000540	中天金融	2021-07-30	0.8242028192650777000000	2.170000	22.9659134799999940000000	1	2564	0
000656	金科股份	2022-03-15	0.5687329013670266000000	3.830000	17.2400000000000060000000	1	2901	0
000671	阳光城	2022-03-15	0.3045692553736344400000	2.060000	45.7311015200000000000000	1	2911	0
000729	燕京啤酒	2020-03-19	1.1396626182775227000000	5.480000	12.3660000000000010000000	1	2975	0
000848	承德露露	2020-04-28	3.1004247664367717000000	6.900000	47.2264000000000200000000	1	2980	0
000869	张裕A	2020-03-23	1.5099602671566557000000	21.810000	37.8550000000000100000000	1	2970	0
000961	中南建设	2022-03-15	0.4165106378766294500000	3.230000	16.5187499999999940000000	1	2945	0
001979	招商蛇口	2021-11-09	0.9217823639187985000000	9.420000	13.0598239000000000000000	1	1524	0
001979	招商蛇口	2021-11-08	0.9217823639187985000000	9.420000	13.0598239000000000000000	2	1524	0
002100	天康生物	2021-07-28	1.2536961556261672000000	7.050000	36.5850600000000100000000	1	2889	0
002124	天邦股份	2021-08-30	1.2299884745251640000000	5.380000	42.2456841500000200000000	1	2923	0
002146	荣盛发展	2022-03-15	0.2913230558298231000000	3.350000	25.3656000000000000000000	1	2942	0
002157	正邦科技	2021-08-30	1.2453729711559203000000	8.410000	45.2320473600000000000000	1	2876	0
002234	民和股份	2021-08-30	1.4288289837881550000000	13.300000	29.5000000000000040000000	1	2966	0


'''







'''
-- 获取股票，股票名称，最新日期，扣非市盈率率，扣非市盈率率低估排名，共有该股票多少交易日记录，当前扣非市盈率率位于历史百分比位置
select raw.stock_code, raw.stock_name, raw.date, raw.pe_ttm_nonrecurring, raw.row_num, record.total_record, raw.percent_num from
(select stock_code, stock_name, date, pe_ttm_nonrecurring,
row_number() OVER (partition by stock_code ORDER BY pe_ttm_nonrecurring asc) AS row_num,
percent_rank() OVER (partition by stock_code ORDER BY pe_ttm_nonrecurring asc) AS percent_num
from stocks_main_estimation_indexes_historical_data
where pe_ttm_nonrecurring>0) raw
left join
(select stock_code, count(*) as total_record from stocks_main_estimation_indexes_historical_data group by stock_code) as record
on raw.stock_code = record.stock_code
where raw.date = (select max(date) from stocks_main_estimation_indexes_historical_data)
and record.total_record>1500
order by raw.percent_num asc;

-- 效果如 
603866	桃李面包	2022-04-07	26.4062387322165720	3	1528	0.0013097576948264572
002511	中顺洁柔	2022-04-07	21.5256754399666370	52	2716	0.01878453038674033
600895	张江高科	2022-04-07	12.0794516852419630	60	2968	0.01988540613414223
601818	光大银行	2022-04-07	4.2020367359736280	60	2805	0.021041369472182596
600015	华夏银行	2022-04-07	3.7188474206319655	77	2957	0.02571041948579161
000876	新希望	2022-04-07	-14.8742832973529890	74	2788	0.026193039110154286
000998	隆平高科	2022-04-07	-120.8900611587226900	93	2808	0.032775204845030284
600873	梅花生物	2022-04-07	13.2214675653753400	93	2781	0.033093525179856115
000537	广宇发展	2022-04-07	-44.8147889443359800	98	2829	0.0342998585572843
000540	中天金融	2022-04-07	-10.2908161401282640	93	2555	0.036021926389976505
300498	温氏股份	2022-04-07	-11.3514109608347430	58	1553	0.03672680412371134
000031	大悦城	2022-04-07	-149.9908804589785200	120	2782	0.042790363178712695
601998	中信银行	2022-04-07	4.4429322212606510	139	2958	0.046668921203922895
601288	农业银行	2022-04-07	4.4845803185909480	137	2844	0.04783679212099894
601169	北京银行	2022-04-07	4.2517218511923530	150	2924	0.05097502565856996
000961	中南建设	2022-04-07	3.4266848226092360	154	2937	0.052111716621253405
601988	中国银行	2022-04-07	4.4738749439161550	156	2962	0.05234718000675447
601328	交通银行	2022-04-07	4.3793931290461080	192	2962	0.06450523471800068
002481	双塔食品	2022-04-07	35.0915181178522700	174	2665	0.06493993993993993
'''




'''
-- 获取股票，股票名称，最新日期，市盈率，市盈率低估排名，共有该股票多少交易日记录，当前市盈率位于历史百分比位置
select raw.stock_code, raw.stock_name, raw.date, raw.pe_ttm, raw.row_num, record.total_record, raw.percent_num from
(select stock_code, stock_name, date, pe_ttm,
row_number() OVER (partition by stock_code ORDER BY pe_ttm asc) AS row_num,
percent_rank() OVER (partition by stock_code ORDER BY pe_ttm asc) AS percent_num
from stocks_main_estimation_indexes_historical_data
where pe_ttm>0) raw
left join
(select stock_code, count(*) as total_record from stocks_main_estimation_indexes_historical_data group by stock_code) as record
on raw.stock_code = record.stock_code
where raw.date = (select max(date) from stocks_main_estimation_indexes_historical_data)
and record.total_record>1500
order by raw.percent_num asc;

stock_code	stock_name	date	pe_ttm	row_num	total_record	percent_num
00700	腾讯控股	2022-04-27	11.7064646471295410000000	2	3035	0.00033068783068783067
002146	荣盛发展	2022-04-27	2.3989549224785240000000	2	2950	0.00033909799932180403
002511	中顺洁柔	2022-04-27	18.6105681708996970000000	2	2733	0.0003661662394727206
603866	桃李面包	2022-04-27	24.1513178494941880000000	2	1544	0.0006480881399870382
000656	金科股份	2022-04-27	2.9845384324964420000000	3	2909	0.0007044734061289186
600848	上海临港	2022-04-27	19.8647774977377400000000	3	2654	0.0008230452674897119
300146	汤臣倍健	2022-04-27	18.7288218320696200000000	3	2591	0.0008507018290089324
600663	陆家嘴	2022-04-27	9.5547115861500240000000	5	2868	0.0013951866062085804
600064	南京高科	2022-04-27	5.0791670862674720000000	6	2985	0.001675603217158177
601818	光大银行	2022-04-27	4.0081734453926785000000	8	2821	0.0024822695035460994
600015	华夏银行	2022-04-27	3.5453218427640256000000	12	2973	0.0033647375504710633
000671	阳光城	2022-04-27	1.9200248464295255000000	16	2919	0.004797806716929404
002461	珠江啤酒	2022-04-27	26.7174377511127550000000	21	2788	0.007176175098672408
600895	张江高科	2022-04-27	10.1544281938150080000000	23	2984	0.0073751257123700975
'''







"""
-- 获取股票，股票名称，最新日期，扣非市盈率率，市净率， 股息率， 扣非市盈率率低估排名，共有该股票多少交易日记录，当前扣非市盈率率位于历史百分比位置
select raw.stock_code, raw.stock_name, raw.date, raw.pe_ttm, raw.pb, raw.dividend_yield, raw.row_num, record.total_record, raw.percent_num from 
(select stock_code, stock_name, date, pe_ttm, pb, dividend_yield,
row_number() OVER (partition by stock_code ORDER BY dividend_yield desc) AS row_num,  
percent_rank() OVER (partition by stock_code ORDER BY dividend_yield desc) AS percent_num
from stocks_main_estimation_indexes_historical_data) raw
left join 
(select stock_code, count(1) as total_record from stocks_main_estimation_indexes_historical_data group by stock_code) as record
on raw.stock_code = record.stock_code
where raw.date = (select max(date) from stocks_main_estimation_indexes_historical_data)
and record.total_record>1500
order by raw.dividend_yield desc;


stock_code	stock_name	date	pe_ttm	pb	dividend_yield	row_num	total_record	percent_num
000671	阳光城	2022-04-25	1.8739442501152170000000	0.3607519335493534000000	0.1549399367363052000000	12	2917	0.0037722908093278463
000961	中南建设	2022-04-25	2.4730460794953752000000	0.5132236342876115000000	0.1395212806519893500000	42	2951	0.013898305084745762
002157	正邦科技	2022-04-25	-2.6062498061208450000000	1.4687788614820538000000	0.1156066332450145000000	4	2882	0.0010413051023950017
000656	金科股份	2022-04-25	3.0820233561709490000000	0.6103112858011697000000	0.1093190674731298000000	6	2907	0.0017205781142463868
002146	荣盛发展	2022-04-25	2.4620853151753272000000	0.3052369928246802500000	0.0997150997150997200000	3	2948	0.0006786562606040041
601155	新城控股	2022-04-25	4.5691857719170560000000	0.9691647512666715000000	0.0804918012607793600000	5	1543	0.0025940337224383916
601328	交通银行	2022-04-25	4.2820562628566700000000	0.4748033752403916000000	0.0702970297029702900000	74	2977	0.024529569892473117
600325	华发股份	2022-04-25	4.4798538146756440000000	0.8452071494177934000000	0.0680473372781065000000	298	2964	0.10023624704691192
601988	中国银行	2022-04-25	4.4452000487537800000000	0.5051552423942249000000	0.0675840978593272200000	215	2977	0.07090053763440861
601288	农业银行	2022-04-25	4.4403962288029430000000	0.5212103390812237000000	0.0675816993464052300000	168	2858	0.05775288764438222
600648	外高桥	2022-04-25	14.9504597681038650000000	1.1985779462090866000000	0.0668881685575364600000	1	2942	0
601169	北京银行	2022-04-25	4.1194764991340490000000	0.4460273364680071300000	0.0666666666708708300000	129	2938	0.04358188627851549
601818	光大银行	2022-04-25	4.0330689326311420000000	0.4688022643213265500000	0.0620370370370370300000	233	2819	0.08197303051809794
601398	工商银行	2022-04-25	4.8600202136222580000000	0.5830804323353220000000	0.0617473684210526340000	320	2977	0.10719086021505377
601998	中信银行	2022-04-25	4.3094252882586590000000	0.4717984925318805000000	0.0616326519390242200000	8	2972	0.0023561090541905083
600000	浦发银行	2022-04-25	4.1643350560246010000000	0.4174582357189858000000	0.0613810505850135160000	296	2943	0.10027192386131883
600064	南京高科	2022-04-25	5.0040314784824504000000	0.7871833206535755000000	0.0600600600600600500000	10	2983	0.0030181086519114686

"""




"""
-- 某个股票代码，当前最新市净率估值，在过去X年中，处于什么百分比
select raw.stock_code, raw.stock_name, raw.date, raw.pb, raw.row_num, record.total_record, raw.percent_num from 
(select stock_code, stock_name, date, pb,
row_number() OVER (partition by stock_code ORDER BY pb asc) AS row_num,  
percent_rank() OVER (partition by stock_code ORDER BY pb asc) AS percent_num
from stocks_main_estimation_indexes_historical_data
where stock_code = "000429"
and `date` > SUBDATE(NOW(),INTERVAL 5 YEAR)) raw
left join 
(select stock_code, count(1) as total_record from stocks_main_estimation_indexes_historical_data where stock_code = "000429" and `date` > SUBDATE(NOW(),INTERVAL 5 YEAR) group by stock_code) as record
on raw.stock_code = record.stock_code
where raw.date = (select max(date) from stocks_main_estimation_indexes_historical_data)
order by raw.percent_num asc;

stock_code	stock_name	date	pb	row_num	total_record	percent_num
000429	粤高速A	2023-05-05	1.8041642748988327000000	674	1214	0.5548227535037098

"""


"""
-- 查看所有监控股票的收集到的最大日期或最小日期
select his_data.stock_code, all_tracking_stocks.stock_name, his_data.p_day
from
(
select stock_code, stock_name, exchange_location, exchange_location_mic
from target_pool.all_tracking_stocks_rf
) as all_tracking_stocks
left join 
(
select stock_code, exchange_location, exchange_location_mic, max(`date`) as p_day
from financial_data.stocks_main_estimation_indexes_historical_data
group by stock_code, exchange_location, exchange_location_mic
) as his_data
on all_tracking_stocks.stock_code = his_data.stock_code
and all_tracking_stocks.exchange_location = his_data.exchange_location
and all_tracking_stocks.exchange_location_mic = his_data.exchange_location_mic

stock_code	stock_name	p_day
600519	贵州茅台	2023-05-08
600809	山西汾酒	2023-05-08
600887	伊利股份	2023-05-08
603288	海天味业	2023-05-08
600132	重庆啤酒	2023-05-08


"""



"""
-- 沪深300指数，当前最新市盈率估值，在过去X年中，处于什么百分比
select raw.index_code, raw.index_name, raw.trading_date, raw.pe, raw.row_num, record.total_record, raw.percent_num from 
(select index_code, index_name, trading_date, pe,
row_number() OVER (partition by index_code ORDER BY pe asc) AS row_num,  
percent_rank() OVER (partition by index_code ORDER BY pe asc) AS percent_num
from stock_bond_ratio_di
where index_code = "000300"
and `trading_date` > SUBDATE(NOW(),INTERVAL 5 YEAR)) raw
left join 
(select index_code, count(1) as total_record from stock_bond_ratio_di where index_code = "000300" and `trading_date` > SUBDATE(NOW(),INTERVAL 5 YEAR) group by index_code) as record
on raw.index_code = record.index_code
where raw.trading_date = (select max(trading_date) from stock_bond_ratio_di)
order by raw.percent_num asc;

index_code	index_name	trading_date	pe	row_num	total_record	percent_num
000300	沪深300	2023-05-15	12.26202317396409700	591	1214	0.48639736191261335
"""



"""
-- 沪深300指数，某个日期市盈率估值，在过去X年中，处于什么百分比
select raw.index_code, raw.index_name, raw.trading_date, raw.pe, raw.row_num, record.total_record, raw.percent_num from 
(select index_code, index_name, trading_date, pe,
row_number() OVER (partition by index_code ORDER BY pe asc) AS row_num,  
percent_rank() OVER (partition by index_code ORDER BY pe asc) AS percent_num
from stock_bond_ratio_di
where index_code = "000300"
and `trading_date` > SUBDATE("2022-10-31",INTERVAL 3 YEAR)) raw
left join 
(select index_code, count(1) as total_record from stock_bond_ratio_di where index_code = "000300" and `trading_date` > SUBDATE("2022-10-31",INTERVAL 3 YEAR) group by index_code) as record
on raw.index_code = record.index_code
where raw.trading_date = "2022-10-31"
order by raw.percent_num asc;

index_code	index_name	trading_date	pe	row_num	total_record	percent_num
000300	沪深300	2022-10-31	10.28601833898130200	1	857	0

"""



"""
-- 基于当前构成，某个指数，过去x年，每一天，有效PE_TTM的值，有效权重，真实pe_ttm, 排行，百分位
select historical_date as p_day, pe_ttm, pe_ttm_effective_weight, (pe_ttm*100/pe_ttm_effective_weight) as full_pe_ttm,
row_number() OVER (partition by index_code ORDER BY (pe_ttm*100/pe_ttm_effective_weight) asc) AS row_num,  
percent_rank() OVER (partition by index_code ORDER BY (pe_ttm*100/pe_ttm_effective_weight) asc) AS percent_num
from aggregated_data.index_components_historical_estimations
where index_code = '399997'
and historical_date  > SUBDATE('2023-06-30',INTERVAL 3 YEAR)
and historical_date <= '2023-06-30';

p_day	pe_ttm	pe_ttm_effective_weight	full_pe_ttm	row_num	percent_num
2022-10-31	25.22863	95.54101	26.406074208	1	0
2022-10-28	25.80334	95.54101	27.007606472	2	0.0013736263736263737
2022-10-27	26.41283	95.54101	27.645541951	3	0.0027472527472527475
2023-06-30	26.52678	95.54101	27.764810106	4	0.004120879120879121
2023-06-07	26.68922	95.54101	27.934831336	5	0.005494505494505495
2023-06-29	26.75985	95.54101	28.008757705	6	0.006868131868131868
2023-06-09	26.81935	95.54101	28.071034627	7	0.008241758241758242
2023-06-08	26.96818	95.54101	28.226810665	8	0.009615384615384616
"""