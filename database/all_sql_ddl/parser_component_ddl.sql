/* --------- user：investor1 ------ */
/* --------- db：parser_component ------ */
/*创建一个表，fake_user_agent，用于存储生成的假UA（用户代理）*/

USE parser_component;
CREATE TABLE IF NOT EXISTS `fake_user_agent`(
	`id` MEDIUMINT NOT NULL AUTO_INCREMENT,
	`ua` VARCHAR(1000) NOT NULL COMMENT '用户代理',
	`submission_date` DATE NOT NULL COMMENT '提交的日期',
	PRIMARY KEY ( `id` )
) ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '生成的假UA（用户代理）';


/* --------- user：investor1 ------ */
/* --------- db：parser_component ------ */
/*创建一个表，token_record，用于存储token */

USE parser_component;
CREATE TABLE IF NOT EXISTS `token_record`(
	`id` MEDIUMINT NOT NULL AUTO_INCREMENT,
	`user` VARCHAR(200) NOT NULL COMMENT '用户',
	`token` VARCHAR(200) NOT NULL COMMENT '身份认证令牌',
	`platform_name` VARCHAR(100) DEFAULT NULL COMMENT '平台中文名称',
	`platform_code` VARCHAR(100) DEFAULT NULL COMMENT '平台代码',
	`bought_date` DATE DEFAULT NULL COMMENT '购买日期',
	`valid_time` VARCHAR(100) DEFAULT NULL COMMENT '有效时间',
	`expire_time` VARCHAR(100) DEFAULT NULL COMMENT '到期时间',
	`p_day` DATE DEFAULT NULL COMMENT '提交的日期',
	`submission_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '提交时间',
	`update_time` TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
	UNIQUE INDEX (token, platform_code),
	PRIMARY KEY ( `id` )
) ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT '身份认证令牌';