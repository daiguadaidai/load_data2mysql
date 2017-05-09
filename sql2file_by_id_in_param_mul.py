#!/usr/bin/env /python
#-*- coding:utf-8 -*-

from multiprocessing import Process
from multiprocessing import Pool
from multiprocessing import Queue
from sqlalchemy import create_engine
import multiprocessing
import pandas as pd
import time
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

class SQL2File(object):
    """执行SQL并将数据导出为一个文件"""
    def __init__(self, username='root', password='root',
                       host='127.0.0.1', port=3306, database='',
                       charset='utf8'):
    
        self.db_conf = {
            'username': username,
            'password': password,
            'host': host,
            'port': port,
            'database': database,
            'charset': charset,
        }

        # 生成数据库连接串
        self.db_str = ('mysql+mysqldb://{username}:{password}@{host}:{port}/{database}'
                       '?charset={charset}'.format(**self.db_conf))
        # 获得连接数据库对象
        self.engine = create_engine(self.db_str)

        self.df = None # 初始化数据结构

    def sql2csv(self, sql='', file_name='/tmp/sql.csv'):
        """执行sql并且导出为Excel
        Args:
            sql: 需要执行的SQL
        """
        self.sql2df(sql = sql)
        self.df.to_csv(file_name)


    def sql2excel(self, sql='', file_name='/tmp/sql.xlsx'):
        """执行sql并且导出为Excel
        Args:
            sql: 需要执行的SQL
        """
        self.sql2df(sql = sql)
        writer = pd.ExcelWriter(file_name, engine='openpyxl')
        self.df.to_excel(writer, 'Sheet1')
        writer.save()

    def sql2df(self, sql=''):
        """执行sql获得 DataFrame对象
        Args:
            sql: 需要执行的SQL
        """

        self.df = pd.read_sql_query(sql, self.engine)
        return self.df

def worker(q, lock, db_conf, file_name, sql):
    """执行sql生成相关文件"""

    print sql
    sql2file = SQL2File(**db_conf)
    sql2file.sql2excel(sql = sql, file_name = file_name)


def main():

    import param_1
    
    manager = multiprocessing.Manager()
    q = manager.Queue()
    lock = manager.Lock()
    p = Pool(processes = 3)


    db_conf = {
        'username': 'root',
        'password': 'root',
        'host': '127.0.0.1',
        'port': 3306,
        'database': 'report',
    }


    for name, account_ids in param_1.account_ids_dict.iteritems():
        excel_name = '/tmp/report/order_detail/{name}.xlsx'.format(name = name)
        account_ids_str = ','.join(['"{account_id}"'.format(account_id=account_id) for account_id in account_ids])

        sql = '''
            SELECT *
            FROM t
            WHERE ID IN({account_ids});
        '''.format(account_ids = account_ids_str)

        p.apply_async(worker, (q, lock, db_conf, excel_name, sql))   

    p.close()
    p.join()



if __name__ == '__main__':
   main()
