import fake_useragent
import sys
import os
sys.path.append("..")
import log.custom_logger as custom_logger
import database.db_operator as db_operator
import time


class GenerateSaveUserAgent:
	# 先删除数据中已过时的数据
	# 随机生成大量UA，并存入数据库
	# 运行频率：每月
	
	def __init__(self):
		pass
		
	def generate_and_save_user_agent(self):
		# 随机生成大量UA，并存入数据库
		
		# 获取当前时间
		today= time.strftime("%Y-%m-%d", time.localtime())


		
		for i in range(2000):
			# 解决总是报 fake_useragent.errors.FakeUserAgentError: Maximum amount of retries reached 问题
			location = os.getcwd() + '/fake_useragent_0.1.11.json'
			# ua = fake_useragent.UserAgent(path=location)

			# 禁用服务器缓存
			# ua = fake_useragent.UserAgent(use_cache_server=False)
			# ua = fake_useragent.UserAgent(cache=False)
			# ua = fake_useragent.UserAgent(verify_ssl=False)

			# 随机生成UA
			ua = fake_useragent.UserAgent(path=location).random
			# 插入数据库
			sql = "INSERT INTO fake_user_agent(ua,submission_date)VALUES ('%s','%s')" %(ua,today)
			db_operator.DBOperator().operate('insert','parser_component', sql)
		
		# 日志记录
		msg = 'Inserted 2000 fake UAs into database'	
		custom_logger.CustomLogger().log_writter(msg,'info')
	
	
	def deleted_outdated_user_agent(self):
		# 删除数据库中过时的UA
		
		# 如果有数据，则删除
		sql = "truncate table fake_user_agent"
		db_operator.DBOperator().operate('delete','parser_component', sql)
		
		# 日志记录
		msg = "Truncate table fake_user_agent to delete all outdated fake UAs from database"
		custom_logger.CustomLogger().log_writter(msg,'info')
	
	
	def deleted_outdated_and_then_generate_and_save_user_agent(self):
		# 先删除数据中已过时的数据
		# 随机生成大量UA，并存入数据库
		
		self.deleted_outdated_user_agent()
		self.generate_and_save_user_agent()


	def main(self):
		self.deleted_outdated_and_then_generate_and_save_user_agent()
		# 日志记录
		msg = 'Just generated fake user agents'
		custom_logger.CustomLogger().log_writter(msg, 'info')

if __name__ == "__main__":
	go = GenerateSaveUserAgent()
	go.deleted_outdated_and_then_generate_and_save_user_agent()

