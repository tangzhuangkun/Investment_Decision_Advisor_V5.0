import time

import sys
sys.path.append("..")
import database.db_operator as db_operator
import log.custom_logger as custom_logger
import db_mapper.aggregated_data.stock_bond_ratio_di_mapper as stock_bond_ratio_di_mapper

class CalculateStockBondRatio:
    # 根据最新的沪深300指数市值加权估值和十年国债收益率，运行mysql脚本，计算股债收益率
    # 运行频率：每天收盘后

    def __init__(self):
        pass

    """
    运行mysql脚本以计算股债收益比
    """
    def run_sql_script_and_cal_ratio(self):

        # 相对路径，是相对于程序执行命令所在的目录，./ 表示的不是脚本所在的目录，而是程序执行命令所在的目录，也就是所谓的当前目录。
        with open("../data_miner/sql_query/cal_stock_bond_ratio.sql", encoding='utf-8', mode='r') as script_f:
            # 分割sql文件中的执行语句，挨句执行
            sql_list = script_f.read().split(';')[:-1]
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
                    msg = '失败，无法成功运行mysql脚本以计算股债收益比' + '  ' + str(e)
                    custom_logger.CustomLogger().log_writter(msg, 'error')

    def main(self):
        # 先清空已计算好的股债比信息表
        stock_bond_ratio_di_mapper.StockBondRatioDiMapper().truncate_table()
        # 运行mysql脚本以计算股债收益比
        self.run_sql_script_and_cal_ratio()

if __name__ == '__main__':
    time_start = time.time()
    go = CalculateStockBondRatio()
    go.main()
    time_end = time.time()
    print('Time Cost: ' + str(time_end - time_start))