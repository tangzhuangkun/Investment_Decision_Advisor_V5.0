/* --------- user：investor1 ------ */
/* --------- db：target_pool ------ */
/*创建一个表，investment_target，用于存储跟踪标的及策略池*/
USE  target_pool;
CREATE TABLE IF NOT EXISTS `investment_target`(
    `id` MEDIUMINT NOT NULL AUTO_INCREMENT,
    `target_type`  VARCHAR(30) NOT NULL COMMENT '跟踪标的类型，如 指数-index，股票-stock',
	`target_code`  VARCHAR(30)  NOT NULL COMMENT '跟踪标的代码，如 399997',
	`target_name`  VARCHAR(50) NOT NULL COMMENT '跟踪标的名称，如 中证白酒指数',
	`index_company`  VARCHAR(20) DEFAULT NULL COMMENT '指数开发公司, 如中证，国证',
    `exchange_location`  VARCHAR(10) NOT NULL COMMENT '标的上市地，如 sh,sz,hk',
    `exchange_location_mic`  VARCHAR(10) NOT NULL COMMENT '标的上市地MIC，如 XSHG, XSHE，XHKG 等',
	`hold_or_not`  tinyint(1) NOT NULL COMMENT '当前是否持有,1为持有，0不持有',
	`trade` VARCHAR(20) NOT NULL COMMENT '交易方向, 买入-buy, 卖出-sell',
	`valuation_method` VARCHAR(20) NOT NULL COMMENT '估值方法, pb,pe,ps,dr,roe,peg 等',
	`trigger_value`  DECIMAL(6,2) NOT NULL COMMENT '估值触发绝对值值临界点，含等于，看指标具体该大于等于还是小于等于，如 pb估值时，0.95',
	`trigger_percent`  DECIMAL(6,2) NOT NULL COMMENT '估值触发历史百分比临界点，含等于，看指标具体该大于等于还是小于等于，如 10，即10%位置',
	`buy_and_hold_strategy`  VARCHAR(100) DEFAULT NULL COMMENT '买入持有策略',
	`sell_out_strategy` VARCHAR(100) DEFAULT NULL COMMENT '卖出策略',
	`monitoring_frequency` VARCHAR(20) NOT NULL COMMENT '监控频率，secondly, minutely, hourly, daily, weekly, monthly, seasonally, yearly, periodically',
	`holder` VARCHAR(100) NOT NULL COMMENT '标的持有人',
	`status` VARCHAR(100) NOT NULL COMMENT '标的监控策略状态，active，suspend，inactive',
	`p_day` DATE DEFAULT NULL COMMENT '提交的日期',
	`submission_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '提交时间',
	`update_time` TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
	UNIQUE INDEX (target_type, target_code, exchange_location, trade, valuation_method, monitoring_frequency, holder),
	PRIMARY KEY ( `id` )
) ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '跟踪标的及策略池';




/* --------- user：investor1 ------ */
/* --------- db：target_pool ------ */
/*创建一个表，all_tracking_stocks_rf，用于存储 所有的需要被跟踪和收集数据的股票，可实时更新*/

USE target_pool;
DROP TABLE IF EXISTS `all_tracking_stocks_rf`;
CREATE TABLE IF NOT EXISTS `all_tracking_stocks_rf`(
	`id` MEDIUMINT NOT NULL AUTO_INCREMENT,
	`stock_code`  VARCHAR(30)  NOT NULL COMMENT '股票代码，如 600519',
	`stock_name`  VARCHAR(50) NOT NULL COMMENT '股票名称，如 贵州茅台',
    `exchange_location`  VARCHAR(10) NOT NULL COMMENT '标的上市地，如 sh,sz,hk',
    `exchange_location_mic`  VARCHAR(10) NOT NULL COMMENT '标的上市地MIC，如 XSHG, XSHE，XHKG 等',
	`p_day` DATE DEFAULT NULL COMMENT '业务日期',
    `submission_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '提交时间',
    `update_time` TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE INDEX (stock_code, exchange_location_mic),
	PRIMARY KEY ( `id` )
	)ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '所有的需要被跟踪和收集数据的股票，可实时更新';



/* --------- user：investor1 ------ */
/* --------- db：target_pool ------ */
/*创建一个表，target_users，用于存储 所有的需要被通知的用户*/

USE target_pool;
DROP TABLE IF EXISTS `target_users`;
CREATE TABLE IF NOT EXISTS `target_users`(
	`id` MEDIUMINT NOT NULL AUTO_INCREMENT,
	`user_name`  VARCHAR(30)  NOT NULL COMMENT '用户名称，如 zhuangkun',
	`contact_id`  VARCHAR(50) NOT NULL COMMENT '通知方式的ID，如 邮箱地址，微信token等',
    `channel`  VARCHAR(50) NOT NULL COMMENT '通知渠道，如 wechat,email',
    `platform_name` VARCHAR(100) DEFAULT NULL COMMENT '平台中文名称',
	`platform_code` VARCHAR(100) DEFAULT NULL COMMENT '平台代码',
    `submission_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '提交时间',
    `update_time` TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE INDEX (user_name, contact_id, channel),
	PRIMARY KEY ( `id` )
	)ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '所有的需要被通知的用户';