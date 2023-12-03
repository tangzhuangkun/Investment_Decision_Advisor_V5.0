/* --------- user：investor1 ------ */
/* --------- db：financial_data ------ */
/*创建一个表，index_constituent_stocks_weights，用于存储 指数构成及权重*/

USE financial_data;
DROP TABLE IF EXISTS `index_constituent_stocks_weight`;
CREATE TABLE IF NOT EXISTS `index_constituent_stocks_weight`(
	`id` MEDIUMINT NOT NULL AUTO_INCREMENT,
	`index_code` VARCHAR(12) NOT NULL COMMENT '指数代码',
	`index_name` VARCHAR(50) NOT NULL COMMENT '指数名称',
	`stock_code` VARCHAR(20) NOT NULL COMMENT '股票代码',
	`stock_name` VARCHAR(20) NOT NULL COMMENT '股票名称',
	`stock_exchange_location` VARCHAR(20) DEFAULT NULL COMMENT '股票上市地',
	`stock_market_code` VARCHAR(20) DEFAULT NULL COMMENT '股票交易市场代码',
	`weight` DECIMAL(21,18) NOT NULL COMMENT '股票权重',
	`source` VARCHAR(10) DEFAULT NULL COMMENT '数据来源',
	`index_company` VARCHAR(20) DEFAULT NULL COMMENT '指数开发公司',
	`p_day` DATE DEFAULT NULL COMMENT '业务日期',
	`submission_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '提交时间',
	UNIQUE INDEX (index_code, stock_code, weight, source, p_day),
	PRIMARY KEY ( `id` )
	)ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '指数构成及权重';


/* --------- user：investor1 ------ */
/* --------- db：financial_data ------ */
/*创建一个表，mix_top10_with_bottom_no_repeat，用于存储 指数成分股及权重构成，最新top10与其它成分股，无重复股*/

USE financial_data;
DROP TABLE IF EXISTS `mix_top10_with_bottom_no_repeat`;
CREATE TABLE IF NOT EXISTS `mix_top10_with_bottom_no_repeat`(
	`id` MEDIUMINT NOT NULL AUTO_INCREMENT,
	`index_code` VARCHAR(12) NOT NULL COMMENT '指数代码',
	`index_name` VARCHAR(50) NOT NULL COMMENT '指数名称',
	`stock_code` VARCHAR(20) NOT NULL COMMENT '股票代码',
	`stock_name` VARCHAR(20) NOT NULL COMMENT '股票名称',
	`stock_exchange_location` VARCHAR(20) DEFAULT NULL COMMENT '股票上市地',
	`stock_market_code` VARCHAR(20) DEFAULT NULL COMMENT '股票交易市场代码',
	`weight` DECIMAL(21,18) NOT NULL COMMENT '股票权重',
	`source` VARCHAR(10) DEFAULT NULL COMMENT '数据来源',
	`index_company` VARCHAR(20) DEFAULT NULL COMMENT '指数开发公司',
	`p_day` DATE DEFAULT NULL COMMENT '业务日期',
	`submission_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '提交时间',
	UNIQUE INDEX (index_code, stock_code, p_day),
	PRIMARY KEY ( `id` )
	)ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '指数成分股及权重构成，最新top10与其它成分股，无重复股';




/* --------- user：investor1 ------ */
/* --------- db：financial_data ------ */
/*创建一个表，stocks_main_estimation_indexes_historical_data，用于存储 股票估值指标历史数据*/

USE financial_data;
DROP TABLE IF EXISTS `stocks_main_estimation_indexes_historical_data`;
CREATE TABLE IF NOT EXISTS `stocks_main_estimation_indexes_historical_data`(
	`id` INT NOT NULL AUTO_INCREMENT,
	`stock_code` VARCHAR(20) NOT NULL COMMENT '股票代码',
	`stock_name` VARCHAR(20) NOT NULL COMMENT '股票名称',
	`date` DATE NOT NULL COMMENT '日期',
	`exchange_location`  VARCHAR(10) NOT NULL COMMENT '标的上市地，如 sh,sz,hk',
    `exchange_location_mic`  VARCHAR(10) NOT NULL COMMENT '标的上市地MIC，如 XSHG, XSHE，XHKG 等',
	`pe_ttm` DECIMAL(28,22) NOT NULL COMMENT '滚动市盈率',
	`pe_ttm_nonrecurring` DECIMAL(24,16) NOT NULL COMMENT '扣非滚动市盈率',
	`pb` DECIMAL(28,22) NOT NULL COMMENT '市净率',
	`pb_wo_gw` DECIMAL(28,22) DEFAULT NULL COMMENT '扣商誉市净率',
	`ps_ttm` DECIMAL(28,22)DEFAULT NULL COMMENT '滚动市销率',
	`pcf_ttm` DECIMAL(28,22) DEFAULT NULL COMMENT '滚动市现率',
	`ev_ebit` DECIMAL(28,22) DEFAULT NULL COMMENT 'EV/EBIT企业价值倍数 ',
	`stock_yield` DECIMAL(28,22) DEFAULT NULL COMMENT '股票收益率',
	`dividend_yield` DECIMAL(28,22) DEFAULT NULL COMMENT '股息率',
	`share_price` DECIMAL(20,6) DEFAULT NULL COMMENT '股价',
	`turnover` BIGINT DEFAULT NULL COMMENT '成交量',
	`fc_rights` DECIMAL(28,22) DEFAULT NULL COMMENT '前复权',
	`bc_rights` DECIMAL(28,22)DEFAULT NULL COMMENT '后复权',
	`lxr_fc_rights` DECIMAL(28,22)DEFAULT NULL COMMENT '理杏仁前复权',
	`shareholders` BIGINT DEFAULT NULL COMMENT '股东人数',
	`market_capitalization` DECIMAL(24,6) DEFAULT NULL COMMENT '市值',
	`circulation_market_capitalization` DECIMAL(24,6) DEFAULT NULL COMMENT '流通市值',
	`free_circulation_market_capitalization` DECIMAL(24,6) DEFAULT NULL COMMENT '自由流通市值',
	`free_circulation_market_capitalization_per_capita` DECIMAL(24,6) DEFAULT NULL COMMENT '人均自由流通市值',
	`financing_balance` DECIMAL(24,6) DEFAULT 0 COMMENT '融资余额',
	`securities_balances` DECIMAL(24,6) DEFAULT 0 COMMENT '融券余额',
	`stock_connect_holding_amount` DECIMAL(24,6) DEFAULT NULL COMMENT '陆股通持仓金额',
	`source` VARCHAR(20) NOT NULL COMMENT '数据来源',
	`submission_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '提交时间',
	UNIQUE INDEX (`stock_code`, `date`, `exchange_location_mic`),
	INDEX (date),
	PRIMARY KEY ( `id` )
	)ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '股票估值指标历史数据';
/* 加索引 */
/*  ALTER TABLE stocks_main_estimation_indexes_historical_data ADD INDEX (date); */


/* --------- user：investor1 ------ */
/* --------- db：financial_data ------ */
/*创建一个表，trading_days，用于存储 交易日期*/

USE financial_data;
DROP TABLE IF EXISTS `trading_days`;
CREATE TABLE IF NOT EXISTS `trading_days`(
	`id` MEDIUMINT NOT NULL AUTO_INCREMENT,
	`trading_date` DATE NOT NULL COMMENT '交易日期',
	`area` VARCHAR(50) NOT NULL COMMENT '地区',
	`source` VARCHAR(10) NOT NULL COMMENT '数据来源',
	`submission_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '提交时间',
	UNIQUE INDEX (trading_date),
	PRIMARY KEY ( `id`)
	)ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '交易日期';



/* --------- user：investor1 ------ */
/* --------- db：financial_data ------ */
/*创建一个表，chn_gov_bonds_rates_di，用于存储 中国国债到期收益率，日增表*/

USE financial_data;
DROP TABLE IF EXISTS `chn_gov_bonds_rates_di`;
CREATE TABLE IF NOT EXISTS `chn_gov_bonds_rates_di`(
	`id` INT NOT NULL AUTO_INCREMENT,
	`1m` VARCHAR(20) NOT NULL COMMENT '1月期限到期利率',
	`2m` VARCHAR(20) DEFAULT NULL COMMENT '2月期限到期利率',
	`3m` VARCHAR(20) DEFAULT NULL COMMENT '3月期限到期利率',
	`6m` VARCHAR(20) DEFAULT NULL COMMENT '6月期限到期利率',
	`9m` VARCHAR(20) DEFAULT NULL COMMENT '9月期限到期利率',
	`1y` VARCHAR(20) DEFAULT NULL COMMENT '1年期限到期利率',
	`2y` VARCHAR(20) DEFAULT NULL COMMENT '2年期限到期利率',
	`3y` VARCHAR(20) DEFAULT NULL COMMENT '3年期限到期利率',
	`5y` VARCHAR(20) DEFAULT NULL COMMENT '5年期限到期利率',
	`7y` VARCHAR(20) DEFAULT NULL COMMENT '7年期限到期利率',
	`10y` VARCHAR(20) DEFAULT NULL COMMENT '10年期限到期利率',
	`trading_day` DATE NOT NULL COMMENT '交易日期',
	`source` VARCHAR(10) NOT NULL COMMENT '数据来源',
	`submission_date` DATE DEFAULT NULL COMMENT '提交的日期',
	`submission_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '提交时间',
	UNIQUE INDEX(trading_day),
	PRIMARY KEY ( `id` )
	)ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '中国国债到期收益率';


/* --------- user：investor1 ------ */
/* --------- db：financial_data ------ */
/*创建一个表，index_estimation_from_lxr，用于存储 理杏仁的指数估值信息*/

USE financial_data;
DROP TABLE IF EXISTS `index_estimation_from_lxr_di`;
CREATE TABLE IF NOT EXISTS `index_estimation_from_lxr_di`(
	`id` MEDIUMINT NOT NULL AUTO_INCREMENT,
	`index_code` VARCHAR(20) NOT NULL COMMENT '指数代码',
	`index_name` VARCHAR(20) NOT NULL COMMENT '指数名称',
	`trading_date` DATE NOT NULL COMMENT '交易日期',
	`tv` BIGINT NOT NULL COMMENT '成交量',
	`ta` BIGINT DEFAULT NULL COMMENT '成交金额',
	`cp` DECIMAL(22,5) NOT NULL COMMENT '收盘点位',
	`cpc` DECIMAL(26,20) NOT NULL COMMENT '涨跌幅',
	`pe_ttm_mcw` DECIMAL(26,17) NOT NULL COMMENT '滚动市盈率市值加权，所有样品公司市值之和 / 所有样品公司归属于母公司净利润之和',
	`pe_ttm_ew` DECIMAL(26,17) NOT NULL COMMENT '滚动市盈率等权，算出所有公司的PE-TTM，然后通过(n / ∑(1 / PE.i))计算出来',
	`pe_ttm_ewpvo` DECIMAL(26,17) NOT NULL COMMENT '滚动市盈率正数等权，剔除所有不赚钱的企业',
	`pe_ttm_avg` DECIMAL(26,17) NOT NULL COMMENT '滚动市盈率平均值，算出所有样品公司的滚动市盈率，剔除负数，然后使用四分位距（interquartile range, IQR）去除极端值，然后加和求平均值',
	`pe_ttm_median` DECIMAL(26,17) NOT NULL COMMENT '滚动市盈率中位数，算出所有样品公司的市盈率，然后排序，然后取最中间的那个数；如果是偶数，那么取中间的两个，加和求半',
	`pb_mcw` DECIMAL(26,17) NOT NULL COMMENT '市净率市值加权，所有样品公司市值之和 / 净资产之和',
	`pb_ew` DECIMAL(26,17) NOT NULL COMMENT '市净率等权，算出所有公司的PB，然后通过(n / ∑(1 / PB.i))计算出来',
	`pb_ewpvo` DECIMAL(26,17) NOT NULL COMMENT '市净率正数等权，剔除所有净资产为负数的企业',
	`pb_avg` DECIMAL(26,17) NOT NULL COMMENT '市净率平均值',
	`pb_median` DECIMAL(26,17) NOT NULL COMMENT '市净率中位数，算出所有样品公司的市净率，然后排序，然后取最中间的那个数；如果是偶数，那么取中间的两个，加和求半',
	`ps_ttm_mcw` DECIMAL(26,17) NOT NULL COMMENT '市销率市值加权，所有样品公司市值之和 / 营业额之和',
	`ps_ttm_ew` DECIMAL(26,17) NOT NULL COMMENT '市销率等权，算出所有公司的PS-TTM，然后通过(n / ∑(1 / PS.i))计算出来',
	`ps_ttm_ewpvo` DECIMAL(26,17) NOT NULL COMMENT '市销率正数等权，剔除所有营业额为0的企业',
	`ps_ttm_avg` DECIMAL(26,17) NOT NULL COMMENT '市销率平均值',
	`ps_ttm_median` DECIMAL(26,17) NOT NULL COMMENT '市销率中位数，算出所有样品公司的市销率，然后排序，然后取最中间的那个数；如果是偶数，那么取中间的两个，加和求半',
	`dyr_mcw` DECIMAL(26,20) NOT NULL COMMENT '股息率市值加权，所有样品公司市值之和 / 分红之和',
	`dyr_ew` DECIMAL(26,20) NOT NULL COMMENT '股息率等权，算出所有公司的DYR，然后通过(n / ∑(1 / DYR.i))计算出来',
	`dyr_ewpvo` DECIMAL(26,20) NOT NULL COMMENT '股息率正数等权，剔除所有不分红的企业',
	`dyr_avg` DECIMAL(26,20) NOT NULL COMMENT '股息率平均值',
	`dyr_median` DECIMAL(26,20) NOT NULL COMMENT '股息率中位数，算出所有样品公司的股息率，然后排序，然后取最中间的那个数；如果是偶数，那么取中间的两个，加和求半',
	`source` VARCHAR(10) NOT NULL COMMENT '数据来源',
	`submission_date` DATE DEFAULT NULL COMMENT '提交的日期',
	`submission_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '提交时间',
	UNIQUE INDEX (index_code, trading_date, source),
	PRIMARY KEY ( `id`)
	)ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '理杏仁的指数估值信息';

/* --------- user：investor1 ------ */
/* --------- db：financial_data ------ */
/*创建一个表，CSI_300_index_stocks，用于存储 中证沪深300指数的成分股*/

/* 暂时不用  2021-11-29 */

/*
USE financial_data;
DROP TABLE IF EXISTS `CSI_300_index_stocks`;
CREATE TABLE IF NOT EXISTS `CSI_300_index_stocks`(
	`id` MEDIUMINT NOT NULL AUTO_INCREMENT,
	`index_code_global` VARCHAR(12) NOT NULL COMMENT '指数代码,含交易所',
	`index_code` VARCHAR(12) NOT NULL COMMENT '指数代码',
	`index_name` VARCHAR(50) NOT NULL COMMENT '指数名称',
	`stock_code_global` VARCHAR(20) DEFAULT NULL COMMENT '股票全球代码',
	`stock_code` VARCHAR(20) NOT NULL COMMENT '股票代码',
	`stock_name` VARCHAR(20) NOT NULL COMMENT '股票名称',
	`stock_weight` DECIMAL(3,2) DEFAULT NULL COMMENT '股票权重',
	`stock_exchange_location` VARCHAR(20) DEFAULT NULL COMMENT '股票上市地',
	`current_price` DECIMAL(12,2) DEFAULT NULL COMMENT '最新价格',
	`eps` DECIMAL(12,2) DEFAULT NULL COMMENT '每股收益',
	`bps` DECIMAL(12,2) DEFAULT NULL COMMENT '每股净资产',
	`roe` DECIMAL(12,2) DEFAULT NULL COMMENT '净资产收益率',
	`total_shares` DECIMAL(12,2) DEFAULT NULL COMMENT '总股本（亿股）',
	`free_shares` DECIMAL(12,2) DEFAULT NULL COMMENT '流通股本（亿股）',
	`free_cap` DECIMAL(12,2) DEFAULT NULL COMMENT '流通市值（亿元）',
	`industry` VARCHAR(20) DEFAULT NULL COMMENT '所属行业',
	`region` VARCHAR(20) DEFAULT NULL COMMENT '地区',
	`source` VARCHAR(10) DEFAULT NULL COMMENT '数据来源',
	`index_company` VARCHAR(20) DEFAULT NULL COMMENT '指数开发公司',
	`p_day` DATE DEFAULT NULL COMMENT '所属日期',
	`submission_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '提交时间',
	UNIQUE INDEX (index_code, stock_code,p_day),
	PRIMARY KEY ( `id` )
	)ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '沪深300指数的成分股';
*/


/* --------- user：investor1 ------ */
/* --------- db：financial_data ------ */
/*创建一个表，excellent_performance_indices_di，用于存储 多年表现优异的指数*/
USE financial_data;
DROP TABLE IF EXISTS `index_excellent_performance_indices_di`;
CREATE TABLE IF NOT EXISTS `index_excellent_performance_indices_di`(
	`id` MEDIUMINT NOT NULL AUTO_INCREMENT,
	`index_code` VARCHAR(60) NOT NULL COMMENT '指数代码',
	`index_name` VARCHAR(60) NOT NULL COMMENT '指数名称',
	`index_company` VARCHAR(60) NOT NULL COMMENT '指数开发公司',
    `one_month_yield_rate` DECIMAL(5,2) DEFAULT NULL COMMENT '近1月年化收益率',
	`three_month_yield_rate` DECIMAL(5,2) DEFAULT NULL COMMENT '近3月年化收益率',
    `this_year_yield_rate` DECIMAL(5,2) DEFAULT NULL COMMENT '今年年化收益率',
	`one_year_yield_rate` DECIMAL(5,2) DEFAULT NULL COMMENT '近1年年化收益率',
	`three_year_yield_rate` DECIMAL(5,2) DEFAULT NULL COMMENT '近3年年化收益率',
	`five_year_yield_rate` DECIMAL(5,2) DEFAULT NULL COMMENT '近5年年化收益率',
    `index_code_tr` VARCHAR(60) DEFAULT NULL COMMENT '全收益指数代码',
	`index_name_tr` VARCHAR(60) DEFAULT NULL COMMENT '全收益指数名称',
    `one_month_yield_rate_tr` DECIMAL(5,2) DEFAULT NULL COMMENT '全收益近1月年化收益率',
	`three_month_yield_rate_tr` DECIMAL(5,2) DEFAULT NULL COMMENT '全收益近3月年化收益率',
    `this_year_yield_rate_tr` DECIMAL(5,2) DEFAULT NULL COMMENT '全收益年至今年化收益率',
	`one_year_yield_rate_tr` DECIMAL(5,2) DEFAULT NULL COMMENT '全收益近1年年化收益率',
    `three_year_yield_rate_tr` DECIMAL(5,2) DEFAULT NULL COMMENT '全收益近3年年化收益率',
	`five_year_yield_rate_tr` DECIMAL(5,2) DEFAULT NULL COMMENT '全收益近5年年化收益率',
	`relative_fund_code` VARCHAR(60) DEFAULT NULL COMMENT '跟踪指数基金代码',
	`relative_fund_name` VARCHAR(60) DEFAULT NULL COMMENT '跟踪指数基金名称',
	`p_day` DATE DEFAULT NULL COMMENT '业务日期',
    `submission_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '提交时间',
    UNIQUE INDEX (index_code, relative_fund_code, p_day),
	PRIMARY KEY ( `id` )
	)ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '表现优异的指数及其跟踪基金';




/* --------- user：investor1 ------ */
/* --------- db：financial_data ------ */
/*创建一个表，fin_data_indexes_list，用于存储 指数代码，名称，成分股个数，指数发行人，数据源*/

USE financial_data;
DROP TABLE IF EXISTS `fin_data_indexes_list`;
CREATE TABLE IF NOT EXISTS `fin_data_indexes_list`(
	`id` MEDIUMINT NOT NULL AUTO_INCREMENT,
	`index_code` VARCHAR(12) NOT NULL COMMENT '指数代码',
    `index_name` VARCHAR(128) NOT NULL COMMENT '指数名称(全称)',
	`index_name_init` VARCHAR(128) DEFAULT NULL COMMENT '指数名称(简称)',
	`securities_num` INT DEFAULT NULL COMMENT '成分股个数',
	`issuer` VARCHAR(20) NOT NULL COMMENT '发行人',
    `source` VARCHAR(20) NOT NULL COMMENT '数据源',
	`submission_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '提交时间',
	UNIQUE INDEX (`index_code`, `source`),
	PRIMARY KEY ( `id` )
	)ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '指数代码，名称，成分股个数，指数发行人';





/* --------- user：investor1 ------ */
/* --------- db：financial_data ------ */
/*创建一个表，fin_data_total_return_indexes_list，用于存储 指数及其可能对应的全收益指数代码，名称，成分股个数，指数发行人，数据源*/

USE financial_data;
DROP TABLE IF EXISTS `fin_data_total_return_indexes_list`;
CREATE TABLE IF NOT EXISTS `fin_data_total_return_indexes_list`(
	`id` MEDIUMINT NOT NULL AUTO_INCREMENT,
	`index_code` VARCHAR(12) NOT NULL COMMENT '指数代码',
    `index_name` VARCHAR(128) NOT NULL COMMENT '指数名称(全称)',
	`index_name_init` VARCHAR(128) DEFAULT NULL COMMENT '指数名称(简称)',
	`issuer` VARCHAR(20) NOT NULL COMMENT '发行人',
    `source` VARCHAR(20) NOT NULL COMMENT '数据源',
    `index_code_tr` VARCHAR(12) NOT NULL COMMENT '相似指数代码',
    `index_name_tr` VARCHAR(128) NOT NULL COMMENT '相似指数名称(全称)',
	`index_name_init_tr` VARCHAR(128) DEFAULT NULL COMMENT '指数名称(简称)',
	`issuer_tr` VARCHAR(20) NOT NULL COMMENT '发行人',
    `source_tr` VARCHAR(20) NOT NULL COMMENT '数据源',
	`submission_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '提交时间',
	UNIQUE INDEX (`index_code`, `source`, `index_code_tr`, `source_tr`),
	PRIMARY KEY ( `id` )
	)ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '指数及其可能对应的全收益指数代码，名称，成分股个数，指数发行人，数据源';