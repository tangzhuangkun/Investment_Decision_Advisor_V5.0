#! /usr/bin/env python3
#coding:utf-8
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import time

import sys
sys.path.append("..")
import log.custom_logger as custom_logger
import conf
import data_miner.data_miner_common_db_operator as data_miner_common_db_operator

class EmailNotification:
	# 通过邮件发送提醒
	
	def __init__(self):
		# 设置发送邮件的参数
		# 设置服务器,第三方 SMTP 服务
		self.email_host = conf.email_host
		# 用户名
		self.email_user = conf.email_user
		# 获取授权码，不是密码
		self.email_pass = conf.email_pass
		# 发件人账号
		self.email_sender = conf.email_sender
		# 接收邮件的账号
		self.email_receivers = data_miner_common_db_operator.DataMinerCommonDBOperator().get_all_channel_users("email")
		# 获取当前时间
		self.today= time.strftime("%Y-%m-%d", time.localtime())
		# 设置邮件主题
		#self.subject = self.today

	
	def send_customized_content(self, object, send_content):
		# 自定义邮件主题+内容
		# param: object, 邮件主题
		# param: send_content, 自定义的内容
		# 发送邮件
		
		# MIMEText 类来实现支持HTML格式的邮件，支持所有HTML格式的元素，包括表格，图片，动画，css样式，表单
		# 第一个参数为邮件内容,第二个设置文本格式，第三个设置编码
		message = MIMEText(send_content, 'plain', 'utf-8')  
		# 发件人
		message['From'] = self.email_sender  
		# 收件人
		message['To'] = ",".join(self.email_receivers)
		# 主题
		message['Subject'] = Header(object, 'utf-8')
	
		try:
			# 创建实例
			smtpObj = smtplib.SMTP_SSL(self.email_host)
			# 连接服务器，25 为 SMTP 端口号
			smtpObj.ehlo(self.email_host)
			# 登录账号
			smtpObj.login(self.email_user, self.email_pass)
			# 发送邮件
			smtpObj.sendmail(self.email_sender, self.email_receivers, message.as_string())
			# 日志记录
			log_msg = 'Success, Sent Email Successfully'
			custom_logger.CustomLogger().log_writter(log_msg, 'info')
		except smtplib.SMTPException as e:
			# 日志记录
			log_msg = 'Failure, Fail To Send Email' + str(e)
			custom_logger.CustomLogger().log_writter(log_msg, 'error')
	
	
if __name__ == '__main__':
	time_start = time.time()
	go = EmailNotification()
	#real_time_pe = go.get_index_real_time_pe('399997')
	send_content = 'hello 2022-07-26  '
	go.send_customized_content(' 基金行情分析test', send_content)
	time_end = time.time()
	print(time_end-time_start)
	