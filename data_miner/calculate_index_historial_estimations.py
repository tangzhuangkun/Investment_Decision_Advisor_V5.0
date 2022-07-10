import time

import sys
sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger


class CalculateIndexHistoricalEstimations:
    # 根据最新的指数成分和股票历史估值信息，运行mysql脚本，计算指数在历史上每一天的估值情况
    # 运行频率：每天收盘后

    def __init__(self):
        pass


    def truncate_table(self):
        # 清空已计算好的估值信息表
        # 插入数据之前，先进行清空操作
        truncating_sql = 'truncate table aggregated_data.index_components_historical_estimations'

        try:
            db_operator.DBOperator().operate("update", "aggregated_data", truncating_sql)

        except Exception as e:
            # 日志记录
            msg = '失败，无法清空 aggregated_data数据库中的index_components_historical_estimations表' + '  ' + str(e)
            custom_logger.CustomLogger().log_writter(msg, 'error')


    def run_file_to_predict_index_latest_component(self):
        # 读取并运行mysql脚本,预测指数的最新构成成分

        # 运行 创建中间层
        # 相对路径，是相对于程序执行命令所在的目录，./ 表示的不是脚本所在的目录，而是程序执行命令所在的目录，也就是所谓的当前目录。
        with open("../data_miner/sql_query/predict_all_indexes_latest_component.sql", encoding='utf-8', mode='r') as view_f:
            # 分割sql文件中的执行语句，挨句执行
            sql_list = view_f.read().split(';')[:-1]
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
                view_inserting_sql = x + ';'

                try:
                    db_operator.DBOperator().operate("insert", "financial_data", view_inserting_sql)

                except Exception as e:
                    # 日志记录
                    msg = '失败，无法成功读取并运行mysql脚本,预测指数的最新构成成分' + '  ' + str(e)
                    custom_logger.CustomLogger().log_writter(msg, 'error')

    def run_file_to_cal_index_his_estimation(self):
        # 运行计算脚本

        # 相对路径，是相对于程序执行命令所在的目录，./ 表示的不是脚本所在的目录，而是程序执行命令所在的目录，也就是所谓的当前目录。
        with open("../data_miner/sql_query/cal_index_his_estimation.sql", encoding='utf-8', mode='r') as f:
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
                    db_operator.DBOperator().operate("insert", "financial_data", inserting_sql)

                except Exception as e:
                    # 日志记录
                    msg = '失败，无法成功将指数的历史估值信息插入 aggregated_data数据库中的index_components_historical_estimations表' + '  ' + str(e)
                    custom_logger.CustomLogger().log_writter(msg, 'error')

    def main(self):
        self.truncate_table()
        self.run_file_to_predict_index_latest_component()
        self.run_file_to_cal_index_his_estimation()

if __name__ == '__main__':
    time_start = time.time()
    go = CalculateIndexHistoricalEstimations()
    go.main()
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))

