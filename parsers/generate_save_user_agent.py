import fake_useragent
import sys
import os
sys.path.append("..")
import log.custom_logger as custom_logger
import time
import db_mapper.parser_component.fake_user_agent_mapper as fake_user_agent_mapper


class GenerateSaveUserAgent:
	# 先删除数据中已过时的数据
	# 随机生成大量UA，并存入数据库
	# 运行频率：每月
	
	def __init__(self):
		pass
		
	def generate_and_save_user_agent(self):
		# 随机生成大量UA，并存入数据库

		for i in range(2000):
			# 解决总是报 fake_useragent.errors.FakeUserAgentError: Maximum amount of retries reached 问题
			#location = os.getcwd() + '/fake_useragent_0.1.11.json'

			# 随机生成UA
			ua = fake_useragent.UserAgent().random
			# 插入数据库
			fake_user_agent_mapper.FakeUserAgentMapper().insert_new_ua(ua)
		
		# 日志记录
		msg = '重新生成2000个新的UA'
		custom_logger.CustomLogger().log_writter(msg,'info')
	
	
	def deleted_outdated_and_then_generate_and_save_user_agent(self):
		# 先删除数据中已过时的数据
		# 随机生成大量UA，并存入数据库

		# 清空数据表以删除过时的假UA
		fake_user_agent_mapper.FakeUserAgentMapper().truncate_table()
		self.generate_and_save_user_agent()

	def main(self):
		self.deleted_outdated_and_then_generate_and_save_user_agent()

if __name__ == "__main__":
	time_start = time.time()
	go = GenerateSaveUserAgent()
	go.main()
	time_end = time.time()
	print('time:')
	print(time_end - time_start)
