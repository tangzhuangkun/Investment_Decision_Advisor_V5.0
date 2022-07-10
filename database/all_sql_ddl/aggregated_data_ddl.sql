/* --------- user：investor1 ------ */
/* --------- db：aggregated_data ------ */
/*创建一个表，index_components_historical_estimations，用于存储 基于指数最新成分股及权重得到的历史每日估值*/

USE aggregated_data;
DROP TABLE IF EXISTS `index_components_historical_estimations`;
CREATE TABLE IF NOT EXISTS `index_components_historical_estimations`(
	`id` int(10) NOT NULL AUTO_INCREMENT,
	`index_code` VARCHAR(12) NOT NULL COMMENT '指数代码',
	`index_name` VARCHAR(50) NOT NULL COMMENT '指数名称',
	`historical_date` DATE NOT NULL COMMENT '历史日期',
	`pe_ttm` DECIMAL(10,5) DEFAULT NULL COMMENT '滚动市盈率',
    `pe_ttm_effective_weight` DECIMAL(10,5) DEFAULT NULL COMMENT '滚动市盈率有效权重',
	`pe_ttm_nonrecurring` DECIMAL(10,5) DEFAULT NULL COMMENT '扣非滚动市盈率',
	`pe_ttm_nonrecurring_effective_weight` DECIMAL(10,5) DEFAULT NULL COMMENT '扣非滚动市盈率有效权重',
	`pb` DECIMAL(10,5) DEFAULT NULL COMMENT '市净率',
	`pb_effective_weight` DECIMAL(10,5) DEFAULT NULL COMMENT '市净率有效权重',
	`pb_wo_gw` DECIMAL(10,5) DEFAULT NULL COMMENT '扣商誉市净率',
	`pb_wo_gw_effective_weight` DECIMAL(10,5) DEFAULT NULL COMMENT '扣商誉市净率有效权重',
	`ps_ttm` DECIMAL(10,5) DEFAULT NULL COMMENT '滚动市销率',
	`ps_ttm_effective_weight` DECIMAL(10,5) DEFAULT NULL COMMENT '滚动市销率有效权重',
	`pcf_ttm` DECIMAL(10,5) DEFAULT NULL COMMENT '滚动市现率',
	`pcf_ttm_effective_weight` DECIMAL(10,5) DEFAULT NULL COMMENT '滚动市现率有效权重',
    `dividend_yield` DECIMAL(10,5) DEFAULT NULL COMMENT '股息率',
    `dividend_yield_effective_weight` DECIMAL(10,5) DEFAULT NULL COMMENT '股息率有效权重',
	`submission_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '提交时间',
	UNIQUE INDEX (index_code, historical_date),
	PRIMARY KEY ( `id` )
	)ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '基于指数最新成分股及权重得到的历史每日估值';


/* --------- user：investor1 ------ */
/* --------- db：aggregated_data ------ */
/*创建一个表，stock_bond_ratio_di，用于存储 每日股债比*/

USE aggregated_data;
DROP TABLE IF EXISTS `stock_bond_ratio_di`;
CREATE TABLE IF NOT EXISTS `stock_bond_ratio_di`(
	`id` int(10) NOT NULL AUTO_INCREMENT,
	`index_code` VARCHAR(12) NOT NULL COMMENT '指数代码',
	`index_name` VARCHAR(50) NOT NULL COMMENT '指数名称',
	`trading_date` DATE NOT NULL COMMENT '交易日期',
	`pe` DECIMAL(22,17) DEFAULT NULL COMMENT '市盈率',
	`stock_yield_rate` DECIMAL(9,6) DEFAULT NULL COMMENT '股票收益率，市盈率倒数',
	`10y_bond_rate` DECIMAL(9,6) DEFAULT NULL COMMENT '10年期国债收益率',
	`ratio` DECIMAL(20,17) DEFAULT NULL COMMENT '股债收益比',
	`submission_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '提交时间',
	UNIQUE INDEX (index_code, trading_date),
	PRIMARY KEY ( `id` )
	)ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '每日股债比';