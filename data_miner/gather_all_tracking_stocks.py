#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun


import time

import sys
sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger

class GatherAllTrackingStocks:
    '''
    聚合汇总所有需要被跟踪的股票
    '''

    def __int__(self):
        pass

    def run_file_to_gather_all_tracking_stocks(self):
        # 运行query脚本

        # 相对路径，是相对于程序执行命令所在的目录，./ 表示的不是脚本所在的目录，而是程序执行命令所在的目录，也就是所谓的当前目录。
        with open("../data_miner/sql_query/gather_all_tracking_stocks_query.sql", encoding='utf-8', mode='r') as f:
            # 读取整个sql文件

            # 分割sql文件中的执行语句，挨句执行
            sql_list = f.read().split(';')[:-1]
            for x in sql_list:
                # 判断包含空行的
                if '\n' in x:
                    # 替换空行为1个空格
                    x = x.replace('\n', ' ')

                # 判断多个空格时
                if '    ' in x:
                    # 替换为空
                    x = x.replace('    ', '')

                # sql语句添加分号结尾
                inserting_sql = x + ';'

                try:
                    db_operator.DBOperator().operate("insert", "target_pool", inserting_sql)

                except Exception as e:
                    # 日志记录
                    msg = '失败，无法成功将指数的历史估值信息插入 aggregated_data数据库中的index_components_historical_estimations表' + '  ' + str(
                        e)
                    custom_logger.CustomLogger().log_writter(msg, 'error')

    def main(self):
        self.run_file_to_gather_all_tracking_stocks()

if __name__ == '__main__':
    time_start = time.time()
    go = GatherAllTrackingStocks()
    go.main()
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))
